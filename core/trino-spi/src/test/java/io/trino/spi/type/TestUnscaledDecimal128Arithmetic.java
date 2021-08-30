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
package io.trino.spi.type;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.math.BigInteger;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.Slices.wrappedIntArray;
import static io.airlift.slice.Slices.wrappedLongArray;
import static io.trino.spi.type.Decimals.MAX_DECIMAL_UNSCALED_VALUE;
import static io.trino.spi.type.Decimals.MIN_DECIMAL_UNSCALED_VALUE;
import static io.trino.spi.type.Decimals.bigIntegerTenToNth;
import static io.trino.spi.type.Decimals.decodeUnscaledValue;
import static io.trino.spi.type.UnscaledDecimal128Arithmetic.add;
import static io.trino.spi.type.UnscaledDecimal128Arithmetic.addWithOverflow;
import static io.trino.spi.type.UnscaledDecimal128Arithmetic.compare;
import static io.trino.spi.type.UnscaledDecimal128Arithmetic.divide;
import static io.trino.spi.type.UnscaledDecimal128Arithmetic.isNegative;
import static io.trino.spi.type.UnscaledDecimal128Arithmetic.isZero;
import static io.trino.spi.type.UnscaledDecimal128Arithmetic.multiply;
import static io.trino.spi.type.UnscaledDecimal128Arithmetic.multiply256Destructive;
import static io.trino.spi.type.UnscaledDecimal128Arithmetic.overflows;
import static io.trino.spi.type.UnscaledDecimal128Arithmetic.rescale;
import static io.trino.spi.type.UnscaledDecimal128Arithmetic.shiftLeft;
import static io.trino.spi.type.UnscaledDecimal128Arithmetic.shiftLeftDestructive;
import static io.trino.spi.type.UnscaledDecimal128Arithmetic.shiftLeftMultiPrecision;
import static io.trino.spi.type.UnscaledDecimal128Arithmetic.shiftRight;
import static io.trino.spi.type.UnscaledDecimal128Arithmetic.shiftRightMultiPrecision;
import static io.trino.spi.type.UnscaledDecimal128Arithmetic.toUnscaledString;
import static io.trino.spi.type.UnscaledDecimal128Arithmetic.unscaledDecimal;
import static io.trino.spi.type.UnscaledDecimal128Arithmetic.unscaledDecimalToBigInteger;
import static io.trino.spi.type.UnscaledDecimal128Arithmetic.unscaledDecimalToUnscaledLong;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestUnscaledDecimal128Arithmetic
{
    private static final Slice MAX_DECIMAL = unscaledDecimal(MAX_DECIMAL_UNSCALED_VALUE);
    private static final Slice MIN_DECIMAL = unscaledDecimal(MIN_DECIMAL_UNSCALED_VALUE);
    private static final BigInteger TWO = BigInteger.valueOf(2);

    @DataProvider
    public static Object[][] testUnscaledBigIntegerToDecimalCases()
    {
        return new Object[][] {
                {MAX_DECIMAL_UNSCALED_VALUE},
                {MIN_DECIMAL_UNSCALED_VALUE},
                {BigInteger.ZERO},
                {BigInteger.ONE},
                {BigInteger.ONE.negate()},
        };
    }

    @Test(dataProvider = "testUnscaledBigIntegerToDecimalCases")
    public void testUnscaledBigIntegerToDecimal(BigInteger value)
    {
        assertConvertsUnscaledBigIntegerToDecimal(value);
    }

    @DataProvider
    public static Object[][] testUnscaledBigIntegerToDecimalOverflowCases()
    {
        return new Object[][] {
                {MAX_DECIMAL_UNSCALED_VALUE.add(BigInteger.ONE)},
                {MAX_DECIMAL_UNSCALED_VALUE.setBit(95)},
                {MAX_DECIMAL_UNSCALED_VALUE.setBit(127)},
                {MIN_DECIMAL_UNSCALED_VALUE.subtract(BigInteger.ONE)},
        };
    }

    @Test(dataProvider = "testUnscaledBigIntegerToDecimalOverflowCases")
    public void testUnscaledBigIntegerToDecimalOverflow(BigInteger value)
    {
        assertUnscaledBigIntegerToDecimalOverflows(value);
    }

    @DataProvider
    public static Object[][] testUnscaledLongToDecimalCases()
    {
        return new Object[][] {
                {0L},
                {1L},
                {-1L},
                {Long.MAX_VALUE},
                {Long.MIN_VALUE},
        };
    }

    @Test(dataProvider = "testUnscaledLongToDecimalCases")
    public void testUnscaledLongToDecimal(long value)
    {
        assertConvertsUnscaledLongToDecimal(value);
    }

    @DataProvider
    public static Object[][] testDecimalToUnscaledLongOverflowCases()
    {
        return new Object[][] {
                {BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE)},
                {BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE)},
                {MAX_DECIMAL_UNSCALED_VALUE},
                {MIN_DECIMAL_UNSCALED_VALUE},
        };
    }

    @Test(dataProvider = "testDecimalToUnscaledLongOverflowCases")
    public void testDecimalToUnscaledLongOverflow(BigInteger value)
    {
        assertDecimalToUnscaledLongOverflows(value);
    }

    @DataProvider
    public static Object[][] testRescaleCases()
    {
        return new Object[][] {
                {unscaledDecimal(10), 0, unscaledDecimal(10L)},
                {unscaledDecimal(-10), 0, unscaledDecimal(-10L)},
                {unscaledDecimal(10), -20, unscaledDecimal(0L)},
                {unscaledDecimal(14), -1, unscaledDecimal(1)},
                {unscaledDecimal(14), -2, unscaledDecimal(0)},
                {unscaledDecimal(14), -3, unscaledDecimal(0)},
                {unscaledDecimal(15), -1, unscaledDecimal(2)},
                {unscaledDecimal(15), -2, unscaledDecimal(0)},
                {unscaledDecimal(15), -3, unscaledDecimal(0)},
                {unscaledDecimal(1050), -3, unscaledDecimal(1)},
                {unscaledDecimal(15), 1, unscaledDecimal(150)},
                {unscaledDecimal(-14), -1, unscaledDecimal(-1)},
                {unscaledDecimal(-14), -2, unscaledDecimal(0)},
                {unscaledDecimal(-14), -20, unscaledDecimal(0)},
                {unscaledDecimal(-15), -1, unscaledDecimal(-2)},
                {unscaledDecimal(-15), -2, unscaledDecimal(0)},
                {unscaledDecimal(-15), -20, unscaledDecimal(0)},
                {unscaledDecimal(-14), 1, unscaledDecimal(-140)},
                {unscaledDecimal(0), 1, unscaledDecimal(0)},
                {unscaledDecimal(0), -1, unscaledDecimal(0)},
                {unscaledDecimal(0), -20, unscaledDecimal(0)},
                {unscaledDecimal(4), -1, unscaledDecimal(0)},
                {unscaledDecimal(5), -1, unscaledDecimal(1)},
                {unscaledDecimal(5), -2, unscaledDecimal(0)},
                {unscaledDecimal(10), 10, unscaledDecimal(100000000000L)},
                {unscaledDecimal("150000000000000000000"), -20, unscaledDecimal(2)},
                {unscaledDecimal("-140000000000000000000"), -20, unscaledDecimal(-1)},
                {unscaledDecimal("50000000000000000000"), -20, unscaledDecimal(1)},
                {unscaledDecimal("150500000000000000000"), -18, unscaledDecimal(151)},
                {unscaledDecimal("-140000000000000000000"), -18, unscaledDecimal(-140)},
                {unscaledDecimal(BigInteger.ONE.shiftLeft(63)), -18, unscaledDecimal(9L)},
                {unscaledDecimal(BigInteger.ONE.shiftLeft(62)), -18, unscaledDecimal(5L)},
                {unscaledDecimal(BigInteger.ONE.shiftLeft(62)), -19, unscaledDecimal(0L)},
                {MAX_DECIMAL, -1, unscaledDecimal(MAX_DECIMAL_UNSCALED_VALUE.divide(BigInteger.TEN).add(BigInteger.ONE))},
                {MIN_DECIMAL, -10, unscaledDecimal(MIN_DECIMAL_UNSCALED_VALUE.divide(BigInteger.valueOf(10000000000L)).subtract(BigInteger.ONE))},
                {unscaledDecimal(1), 37, unscaledDecimal("10000000000000000000000000000000000000")},
                {unscaledDecimal(-1), 37, unscaledDecimal("-10000000000000000000000000000000000000")},
                {unscaledDecimal("10000000000000000000000000000000000000"), -37, unscaledDecimal(1)},
        };
    }

    @Test(dataProvider = "testRescaleCases")
    public void testRescale(Slice decimal, int rescale, Slice expected)
    {
        assertRescale(decimal, rescale, expected);
    }

    @DataProvider
    public static Object[][] testRescaleOverflowsCases()
    {
        return new Object[][] {
                {unscaledDecimal(1), 38},
        };
    }

    @Test(dataProvider = "testRescaleOverflowsCases")
    public void testRescaleOverflows(Slice decimal, int rescaleFactor)
    {
        assertRescaleOverflows(decimal, rescaleFactor);
    }

    @DataProvider
    public static Object[][] testAddCases()
    {
        return new Object[][] {
                {unscaledDecimal(0), unscaledDecimal(0), unscaledDecimal(0)},
                {unscaledDecimal(1), unscaledDecimal(0), unscaledDecimal(1)},
                {unscaledDecimal(1), unscaledDecimal(1), unscaledDecimal(2)},
                {unscaledDecimal(-1), unscaledDecimal(0), unscaledDecimal(-1)},
                {unscaledDecimal(-1), unscaledDecimal(-1), unscaledDecimal(-2)},
                {unscaledDecimal(-1), unscaledDecimal(1), unscaledDecimal(0)},
                {unscaledDecimal(1), unscaledDecimal(-1), unscaledDecimal(0)},
                {unscaledDecimal("10000000000000000000000000000000000000"), unscaledDecimal(0), unscaledDecimal("10000000000000000000000000000000000000")},
                {unscaledDecimal("10000000000000000000000000000000000000"), unscaledDecimal("10000000000000000000000000000000000000"), unscaledDecimal("20000000000000000000000000000000000000")},
                {unscaledDecimal("-10000000000000000000000000000000000000"), unscaledDecimal(0), unscaledDecimal("-10000000000000000000000000000000000000")},
                {unscaledDecimal("-10000000000000000000000000000000000000"), unscaledDecimal("-10000000000000000000000000000000000000"), unscaledDecimal("-20000000000000000000000000000000000000")},
                {unscaledDecimal("-10000000000000000000000000000000000000"), unscaledDecimal("10000000000000000000000000000000000000"), unscaledDecimal(0)},
                {unscaledDecimal("10000000000000000000000000000000000000"), unscaledDecimal("-10000000000000000000000000000000000000"), unscaledDecimal(0)},

                {unscaledDecimal(1L << 32), unscaledDecimal(0), unscaledDecimal(1L << 32)},
                {unscaledDecimal(1L << 31), unscaledDecimal(1L << 31), unscaledDecimal(1L << 32)},
                {unscaledDecimal(1L << 32), unscaledDecimal(1L << 33), unscaledDecimal((1L << 32) + (1L << 33))},
        };
    }

    @Test(dataProvider = "testAddCases")
    public void testAdd(Slice left, Slice right, Slice expected)
    {
        assertEquals(add(left, right), expected);
    }

    @DataProvider
    public static Object[][] testAddReturnOverflowCases()
    {
        return new Object[][] {
                {TWO, TWO},
                {MAX_DECIMAL_UNSCALED_VALUE, MAX_DECIMAL_UNSCALED_VALUE},
                {MAX_DECIMAL_UNSCALED_VALUE.negate(), MAX_DECIMAL_UNSCALED_VALUE},
                {MAX_DECIMAL_UNSCALED_VALUE, MAX_DECIMAL_UNSCALED_VALUE.negate()},
                {MAX_DECIMAL_UNSCALED_VALUE.negate(), MAX_DECIMAL_UNSCALED_VALUE.negate()},
        };
    }

    @Test(dataProvider = "testAddReturnOverflowCases")
    public void testAddReturnOverflow(BigInteger left, BigInteger right)
    {
        assertAddReturnOverflow(left, right);
    }

    @DataProvider
    public static Object[][] testMultiplyCases()
    {
        return new Object[][] {
                {0, 0, 0},
                {1, 0, 0},
                {0, 1, 0},
                {-1, 0, 0},
                {0, -1, 0},
                {1, 1, 1},
                {1, -1, -1},
                {-1, -1, 1},
                {MAX_DECIMAL_UNSCALED_VALUE, 0, 0},
                {MAX_DECIMAL_UNSCALED_VALUE, 1, MAX_DECIMAL_UNSCALED_VALUE},
                {MIN_DECIMAL_UNSCALED_VALUE, 0, 0},
                {MIN_DECIMAL_UNSCALED_VALUE, 1, MIN_DECIMAL_UNSCALED_VALUE},
                {MAX_DECIMAL_UNSCALED_VALUE, -1, MIN_DECIMAL_UNSCALED_VALUE},
                {MIN_DECIMAL_UNSCALED_VALUE, -1, MAX_DECIMAL_UNSCALED_VALUE},
                {new BigInteger("FFFFFFFFFFFFFFFF", 16), new BigInteger("FFFFFFFFFFFFFF", 16), new BigInteger("fffffffffffffeff00000000000001", 16)},
                {new BigInteger("FFFFFF0096BFB800", 16), new BigInteger("39003539D9A51600", 16), new BigInteger("39003500FB00AB761CDBB17E11D00000", 16)},
                {Integer.MAX_VALUE, Integer.MIN_VALUE, (long) Integer.MAX_VALUE * Integer.MIN_VALUE},
                {new BigInteger("99999999999999"), new BigInteger("-1000000000000000000000000"), new BigInteger("-99999999999999000000000000000000000000")},
                {new BigInteger("12380837221737387489365741632769922889"), 3, new BigInteger("37142511665212162468097224898309768667")},
        };
    }

    @Test(dataProvider = "testMultiplyCases")
    public void testMultiply(Number a, Number b, Number result)
    {
        assertMultiply(
                a instanceof BigInteger ? (BigInteger) a : BigInteger.valueOf(a.longValue()),
                b instanceof BigInteger ? (BigInteger) b : BigInteger.valueOf(b.longValue()),
                result instanceof BigInteger ? (BigInteger) result : BigInteger.valueOf(result.longValue()));
    }

    @DataProvider
    public static Object[][] testMultiply256Cases()
    {
        return new Object[][] {
                {MAX_DECIMAL, MAX_DECIMAL, wrappedLongArray(0xECEBBB8000000001L, 0xE0FF0CA0BC87870BL, 0x0764B4ABE8652978L, 0x161BCCA7119915B5L)},
                {wrappedLongArray(0xFFFFFFFFFFFFFFFFL, 0x0FFFFFFFFFFFFFFFL), wrappedLongArray(0xFFFFFFFFFFFFFFFFL, 0x0FFFFFFFFFFFFFFFL),
                        wrappedLongArray(0x0000000000000001L, 0xE000000000000000L, 0xFFFFFFFFFFFFFFFFL, 0x00FFFFFFFFFFFFFFL)},
                {wrappedLongArray(0x1234567890ABCDEFL, 0x0EDCBA0987654321L), wrappedLongArray(0xFEDCBA0987654321L, 0x1234567890ABCDEL),
                        wrappedLongArray(0xC24A442FE55618CFL, 0xAA71A60D0DA49DDAL, 0x7C163D5A13DF8695L, 0x0010E8EEF9BD1294L)},
        };
    }

    @Test(dataProvider = "testMultiply256Cases")
    public void testMultiply256(Slice left, Slice right, Slice expected)
    {
        assertMultiply256(left, right, expected);
    }

    private static void assertMultiply256(Slice left, Slice right, Slice expected)
    {
        int[] leftArg = new int[8];
        leftArg[0] = left.getInt(0);
        leftArg[1] = left.getInt(4);
        leftArg[2] = left.getInt(8);
        leftArg[3] = left.getInt(12);

        multiply256Destructive(leftArg, right);
        assertEquals(wrappedIntArray(leftArg), expected);
    }

    @DataProvider
    public static Object[][] testMultiplyOverflowCases()
    {
        return new Object[][] {
                {unscaledDecimal("99999999999999"), unscaledDecimal("-10000000000000000000000000")},
                {MAX_DECIMAL, unscaledDecimal("10")},
        };
    }

    @Test(dataProvider = "testMultiplyOverflowCases")
    public void testMultiplyOverflow(Slice left, Slice right)
    {
        assertMultiplyOverflows(left, right);
    }

    @DataProvider
    public static Object[][] testShiftRightCases()
    {
        return new Object[][] {
                {unscaledDecimal(0), 0, true, unscaledDecimal(0)},
                {unscaledDecimal(0), 33, true, unscaledDecimal(0)},

                {unscaledDecimal(1), 1, true, unscaledDecimal(1)},
                {unscaledDecimal(1), 1, false, unscaledDecimal(0)},
                {unscaledDecimal(1), 2, true, unscaledDecimal(0)},
                {unscaledDecimal(1), 2, false, unscaledDecimal(0)},
                {unscaledDecimal(-4), 1, true, unscaledDecimal(-2)},
                {unscaledDecimal(-4), 1, false, unscaledDecimal(-2)},
                {unscaledDecimal(-4), 2, true, unscaledDecimal(-1)},
                {unscaledDecimal(-4), 2, false, unscaledDecimal(-1)},
                {unscaledDecimal(-4), 3, true, unscaledDecimal(-1)},
                {unscaledDecimal(-4), 3, false, unscaledDecimal(0)},
                {unscaledDecimal(-4), 4, true, unscaledDecimal(0)},
                {unscaledDecimal(-4), 4, false, unscaledDecimal(0)},

                {unscaledDecimal(1L << 32), 32, true, unscaledDecimal(1)},
                {unscaledDecimal(1L << 31), 32, true, unscaledDecimal(1)},
                {unscaledDecimal(1L << 31), 32, false, unscaledDecimal(0)},
                {unscaledDecimal(3L << 33), 34, true, unscaledDecimal(2)},
                {unscaledDecimal(3L << 33), 34, false, unscaledDecimal(1)},
                {unscaledDecimal(BigInteger.valueOf(0x7FFFFFFFFFFFFFFFL).setBit(63).setBit(64)), 1, true, unscaledDecimal(BigInteger.ONE.shiftLeft(64))},

                {MAX_DECIMAL, 1, true, unscaledDecimal(MAX_DECIMAL_UNSCALED_VALUE.shiftRight(1).add(BigInteger.ONE))},
                {MIN_DECIMAL, 1, true, unscaledDecimal(MAX_DECIMAL_UNSCALED_VALUE.shiftRight(1).add(BigInteger.ONE).negate())},
                {MAX_DECIMAL, 66, true, unscaledDecimal(MAX_DECIMAL_UNSCALED_VALUE.shiftRight(66).add(BigInteger.ONE))},
        };
    }

    @Test(dataProvider = "testShiftRightCases")
    public void testShiftRight(Slice decimal, int rightShifts, boolean roundUp, Slice expectedResult)
    {
        assertShiftRight(decimal, rightShifts, roundUp, expectedResult);
    }

    @DataProvider
    public static Object[][] testDivideSimpleCases()
    {
        return new Object[][] {
                {"0", "10"},
                {"5", "10"},
                {"50", "100"},
                {"99", "10"},
                {"95", "10"},
                {"91", "10"},
                {"1000000000000000000000000", "10"},
                {"1000000000000000000000000", "3"},
                {"1000000000000000000000000", "9"},
                {"1000000000000000000000000", "100000000000000000000000"},
                {"1000000000000000000000000", "333333333333333333333333"},
                {"1000000000000000000000000", "111111111111111111111111"},
        };
    }

    @Test(dataProvider = "testDivideSimpleCases")
    public void testDivideSimple(String dividend, String divisor)
    {
        assertDivideAllSigns(dividend, divisor);
    }

    @DataProvider
    public static Object[][] testDivideCases()
    {
        return new Object[][] {
                // dividend < divisor
                {new int[] {4, 3, 2, 0}, new int[] {4, 3, 2, 1}},
                {new int[] {4, 3, 0, 0}, new int[] {4, 3, 2, 0}},
                {new int[] {4, 0, 0, 0}, new int[] {4, 3, 0, 0}},
                {new int[] {0, 0, 0, 0}, new int[] {4, 0, 0, 0}},

                // different lengths
                {new int[] {1423957378, 1765820914, 0xFFFFFFFF, 0}, new int[] {4, 0x0000FFFF, 0, 0}},
                {new int[] {1423957378, 1765820914, 0xFFFFFFFF, 0}, new int[] {2042457708, 0, 0, 0}},
                {new int[] {1423957378, -925263858, 0, 0}, new int[] {2042457708, 0, 0, 0}},
                {new int[] {0xFFFFFFFF, 0, 0, 0}, new int[] {2042457708, 0, 0, 0}},

                // single int divisor
                {new int[] {1423957378, -1444436990, -925263858, 1106345725}, new int[] {2042457708, 0, 0, 0}},
                {new int[] {0, 0xF7000000, 0, 0x39000000}, new int[] {-1765820914, 0, 0, 0}},

                // normalization scale = 1
                {new int[] {0x0FF00210, 0xF7001230, 0xFB00AC00, 0x39003500}, new int[] {-1765820914, 2042457708, 0xFFFFFFFF, 0}},
                {new int[] {0x0FF00210, 0xF7001230, 0xFB00AC00, 0x39003500}, new int[] {-1765820914, 0xFFFFFF00, 0, 0}},
                {new int[] {0x0FF00210, 0xF7001230, 0xFB00AC00, 0x39003500}, new int[] {-1765820914, 0xFF000000, 0, 0}},

                // normalization scale > 1
                {new int[] {0x0FF00210, 0xF7001230, 0xFB00AC00, 0x39003500}, new int[] {-1765820914, 2042457708, 0xFFFFFFFF, 0x7FFFFFFF}},
                {new int[] {0x0FF00210, 0xF7001230, 0xFB00AC00, 0x39003500}, new int[] {-1765820914, 2042457708, 0x4FFFFFFF, 0}},
                {new int[] {0x0FF00210, 0xF7001230, 0xFB00AC00, 0x39003500}, new int[] {-1765820914, 2042457708, 0x0000FFFF, 0}},

                // normalization scale signed overflow
                {new int[] {1, 1, 1, 0x7FFFFFFF}, new int[] {0xFFFFFFFF, 1, 0, 0}},

                // u2 = v1
                {new int[] {0, 0x8FFFFFFF, 0x8FFFFFFF, 0}, new int[] {0xFFFFFFFF, 0x8FFFFFFF, 0, 0}},

                // qhat is greater than q by 1
                {new int[] {1, 1, 0xFFFFFFFF, 0}, new int[] {0xFFFFFFFF, 0x7FFFFFFF, 0, 0}},

                // qhat is greater than q by 2
                {new int[] {1, 1, 0xFFFFFFFF, 0}, new int[] {0xFFFFFFFF, 0x7FFFFFFF, 0, 0}},

                // overflow after multiplyAndSubtract
                {new int[] {0x00000003, 0x00000000, 0x80000000, 0}, new int[] {0x00000001, 0x00000000, 0x20000000, 0}},
                {new int[] {0x00000003, 0x00000000, 0x00008000, 0}, new int[] {0x00000001, 0x00000000, 0x00002000, 0}},
                {new int[] {0, 0, 0x00008000, 0x00007fff}, new int[] {1, 0, 0x00008000, 0}},

                // test cases from http://www.hackersdelight.org/hdcodetxt/divmnu64.c.txt
                // license: http://www.hackersdelight.org/permissions.htm
                {new int[] {3, 0, 0, 0}, new int[] {2, 0, 0, 0}},
                {new int[] {3, 0, 0, 0}, new int[] {3, 0, 0, 0}},
                {new int[] {3, 0, 0, 0}, new int[] {4, 0, 0, 0}},
                {new int[] {3, 0, 0, 0}, new int[] {0xffffffff, 0, 0, 0}},
                {new int[] {0xffffffff, 0, 0, 0}, new int[] {1, 0, 0, 0}},
                {new int[] {0xffffffff, 0, 0, 0}, new int[] {0xffffffff, 0, 0, 0}},
                {new int[] {0xffffffff, 0, 0, 0}, new int[] {3, 0, 0, 0}},
                {new int[] {0xffffffff, 0xffffffff, 0, 0}, new int[] {1, 0, 0, 0}},
                {new int[] {0xffffffff, 0xffffffff, 0, 0}, new int[] {0xffffffff, 0, 0, 0}},
                {new int[] {0xffffffff, 0xfffffffe, 0, 0}, new int[] {0xffffffff, 0, 0, 0}},
                {new int[] {0x00005678, 0x00001234, 0, 0}, new int[] {0x00009abc, 0, 0, 0}},
                {new int[] {0, 0, 0, 0}, new int[] {0, 1, 0, 0}},
                {new int[] {0, 7, 0, 0}, new int[] {0, 3, 0, 0}},
                {new int[] {5, 7, 0, 0}, new int[] {0, 3, 0, 0}},
                {new int[] {0, 6, 0, 0}, new int[] {0, 2, 0, 0}},
                {new int[] {0x80000000, 0, 0, 0}, new int[] {0x40000001, 0, 0, 0}},
                {new int[] {0x00000000, 0x80000000, 0, 0}, new int[] {0x40000001, 0, 0, 0}},
                {new int[] {0x00000000, 0x80000000, 0, 0}, new int[] {0x00000001, 0x40000000, 0, 0}},
                {new int[] {0x0000789a, 0x0000bcde, 0, 0}, new int[] {0x0000789a, 0x0000bcde, 0, 0}},
                {new int[] {0x0000789b, 0x0000bcde, 0, 0}, new int[] {0x0000789a, 0x0000bcde, 0, 0}},
                {new int[] {0x00007899, 0x0000bcde, 0, 0}, new int[] {0x0000789a, 0x0000bcde, 0, 0}},
                {new int[] {0x0000ffff, 0x0000ffff, 0, 0}, new int[] {0x0000ffff, 0x0000ffff, 0, 0}},
                {new int[] {0x0000ffff, 0x0000ffff, 0, 0}, new int[] {0x00000000, 0x0000ffff, 0, 0}},
                {new int[] {0x000089ab, 0x00004567, 0x00000123, 0}, new int[] {0x00000000, 0x00000001, 0, 0}},
                {new int[] {0x000089ab, 0x00004567, 0x00000123, 0}, new int[] {0x00000000, 0x00000001, 0, 0}},
                {new int[] {0x00000000, 0x0000fffe, 0x00008000, 0}, new int[] {0x0000ffff, 0x00008000, 0, 0}},
                {new int[] {0x00000003, 0x00000000, 0x80000000, 0}, new int[] {0x00000001, 0x00000000, 0x20000000, 0}},
                {new int[] {0x00000003, 0x00000000, 0x00008000, 0}, new int[] {0x00000001, 0x00000000, 0x00002000, 0}},
                {new int[] {0, 0, 0x00008000, 0x00007fff}, new int[] {1, 0, 0x00008000, 0}},
                {new int[] {0, 0x0000fffe, 0, 0x00008000}, new int[] {0x0000ffff, 0, 0x00008000, 0}},
                {new int[] {0, 0xfffffffe, 0, 0x80000000}, new int[] {0x0000ffff, 0, 0x80000000, 0}},
                {new int[] {0, 0xfffffffe, 0, 0x80000000}, new int[] {0xffffffff, 0, 0x80000000, 0}},
        };
    }

    @Test(dataProvider = "testDivideCases")
    public void testDivide(int[] dividend, int[] divisor)
    {
        assertDivideAllSigns(dividend, divisor);
    }

    @DataProvider
    public static Object[][] testDivideWithRescaleCases()
    {
        return new Object[][] {
                {"100000000000000000000000", 10, "111111111111111111111111", 10},
                {"100000000000000000000000", 10, "111111111111", 22},
                {"99999999999999999999999999999999999999", 37, "99999999999999999999999999999999999999", 37},
                {"99999999999999999999999999999999999999", 2, "99999999999999999999999999999999999999", 1},
                {"99999999999999999999999999999999999999", 37, "9", 37},
                {"99999999999999999999999999999999999999", 37, "1", 37},
                {"11111111111111111111111111111111111111", 37, "2", 37},
                {"11111111111111111111111111111111111111", 37, "2", 1},
                {"97764425639372288753711864842425458618", 36, "32039006229599111733094986468789901155", 0},
                {"34354576602352622842481633786816220283", 0, "31137583115118564930544829855652258045", 0},
                {"96690614752287690630596513604374991473", 0, "10039352042372909488692220528497751229", 0},
                {"87568357716090115374029040878755891076", 0, "46106713604991337798209343815577148589", 0},
        };
    }

    @Test(dataProvider = "testDivideWithRescaleCases")
    public void testDivideWithRescale(String dividend, int dividendRescaleFactor, String divisor, int divisorRescaleFactor)
    {
        assertDivideAllSigns(dividend, dividendRescaleFactor, divisor, divisorRescaleFactor);
    }

    @DataProvider
    public static Object[][] testOverflowsCases()
    {
        return new Object[][] {
                {"100", 2, true},
                {"-100", 2, true},
                {"99", 2, false},
                {"-99", 2, false},
        };
    }

    @Test(dataProvider = "testOverflowsCases")
    public void testOverflows(String unscaledValue, int precision, boolean expected)
    {
        assertEquals(overflows(unscaledDecimal(unscaledValue), precision), expected);
    }

    @DataProvider
    public static Object[][] testCompareCases()
    {
        return new Object[][] {
                {0, 0, 0},

                {0, 10, -1},
                {10, 0, 1},

                {-10, -11, 1},
                {-11, -11, 0},
                {-12, -11, -1},

                {10, 11, -1},
                {11, 11, 0},
                {12, 11, 1},
        };
    }

    @Test(dataProvider = "testCompareCases")
    public void testCompare(long left, long right, int expectedResult)
    {
        assertCompare(unscaledDecimal(left), unscaledDecimal(right), expectedResult);
    }

    @DataProvider
    public static Object[][] testNegateCases()
    {
        return new Object[][] {
                {negate(negate(MIN_DECIMAL)), MIN_DECIMAL},
                {negate(MIN_DECIMAL), MAX_DECIMAL},
                {negate(MIN_DECIMAL), MAX_DECIMAL},

                {negate(unscaledDecimal(1)), unscaledDecimal(-1)},
                {negate(unscaledDecimal(-1)), unscaledDecimal(1)},
        };
    }

    @Test(dataProvider = "testNegateCases")
    public void testNegate(Slice actual, Slice expected)
    {
        assertEquals(actual, expected);
    }

    @DataProvider
    public static Object[][] testIsNegativeCases()
    {
        return new Object[][] {
                {MIN_DECIMAL, true},
                {MAX_DECIMAL, false},
                {unscaledDecimal(0), false},
        };
    }

    @Test(dataProvider = "testIsNegativeCases")
    public void testIsNegative(Slice decimal, boolean expected)
    {
        assertEquals(isNegative(decimal), expected);
    }

    @DataProvider
    public static Object[][] testToStringCases()
    {
        return new Object[][] {
                {unscaledDecimal(0), "0"},
                {unscaledDecimal(1), "1"},
                {unscaledDecimal(-1), "-1"},
                {unscaledDecimal(MAX_DECIMAL), MAX_DECIMAL_UNSCALED_VALUE.toString()},
                {unscaledDecimal(MIN_DECIMAL), MIN_DECIMAL_UNSCALED_VALUE.toString()},
                {unscaledDecimal("1000000000000000000000000000000000000"), "1000000000000000000000000000000000000"},
                {unscaledDecimal("-1000000000002000000000000300000000000"), "-1000000000002000000000000300000000000"},
        };
    }

    @Test(dataProvider = "testToStringCases")
    public void testToString(Slice decimal, String expected)
    {
        assertEquals(toUnscaledString(decimal), expected);
    }

    @DataProvider
    public static Object[][] testShiftLeftMultiPrecisionCases()
    {
        return new Object[][] {
                {
                        new int[] {0b10100001010001011010000101000101, 0b01010110100101101011010101010101, 0b01010010111110001111100010101010,
                                0b11111111000000011010101010101011, 0b00000000000000000000000000000000},
                        4,
                        0,
                        new int[] {0b10100001010001011010000101000101, 0b01010110100101101011010101010101, 0b01010010111110001111100010101010,
                                0b11111111000000011010101010101011, 0b00000000000000000000000000000000}
                },
                {
                        new int[] {0b10100001010001011010000101000101, 0b01010110100101101011010101010101, 0b01010010111110001111100010101010,
                                0b11111111000000011010101010101011, 0b00000000000000000000000000000000},
                        5,
                        1,
                        new int[] {0b01000010100010110100001010001010, 0b10101101001011010110101010101011, 0b10100101111100011111000101010100,
                                0b11111110000000110101010101010110, 0b00000000000000000000000000000001}
                },
                {
                        new int[] {0b10100001010001011010000101000101, 0b01010110100101101011010101010101, 0b01010010111110001111100010101010,
                                0b11111111000000011010101010101011, 0b00000000000000000000000000000000},
                        5,
                        31,
                        new int[] {0b10000000000000000000000000000000, 0b11010000101000101101000010100010, 0b00101011010010110101101010101010,
                                0b10101001011111000111110001010101, 0b1111111100000001101010101010101}
                },
                {
                        new int[] {0b10100001010001011010000101000101, 0b01010110100101101011010101010101, 0b01010010111110001111100010101010,
                                0b11111111000000011010101010101011, 0b00000000000000000000000000000000},
                        5,
                        32,
                        new int[] {0b00000000000000000000000000000000, 0b10100001010001011010000101000101, 0b01010110100101101011010101010101,
                                0b01010010111110001111100010101010, 0b11111111000000011010101010101011}
                },
                {
                        new int[] {0b10100001010001011010000101000101, 0b01010110100101101011010101010101, 0b01010010111110001111100010101010,
                                0b11111111000000011010101010101011, 0b00000000000000000000000000000000, 0b00000000000000000000000000000000},
                        6,
                        33,
                        new int[] {0b00000000000000000000000000000000, 0b01000010100010110100001010001010, 0b10101101001011010110101010101011,
                                0b10100101111100011111000101010100, 0b11111110000000110101010101010110, 0b00000000000000000000000000000001}
                },
                {
                        new int[] {0b10100001010001011010000101000101, 0b01010110100101101011010101010101, 0b01010010111110001111100010101010,
                                0b11111111000000011010101010101011, 0b00000000000000000000000000000000, 0b00000000000000000000000000000000},
                        6,
                        37,
                        new int[] {0b00000000000000000000000000000000, 0b00101000101101000010100010100000, 0b11010010110101101010101010110100,
                                0b01011111000111110001010101001010, 0b11100000001101010101010101101010, 0b00000000000000000000000000011111}
                },
                {
                        new int[] {0b10100001010001011010000101000101, 0b01010110100101101011010101010101, 0b01010010111110001111100010101010,
                                0b11111111000000011010101010101011, 0b00000000000000000000000000000000, 0b00000000000000000000000000000000},
                        6,
                        64,
                        new int[] {0b00000000000000000000000000000000, 0b00000000000000000000000000000000, 0b10100001010001011010000101000101,
                                0b01010110100101101011010101010101, 0b01010010111110001111100010101010, 0b11111111000000011010101010101011}
                },
        };
    }

    @Test(dataProvider = "testShiftLeftMultiPrecisionCases")
    public void testShiftLeftMultiPrecision(int[] number, int length, int shifts, int[] expected)
    {
        assertEquals(shiftLeftMultiPrecision(number, length, shifts), expected);
    }

    @DataProvider
    public static Object[][] testShiftRightMultiPrecisionCases()
    {
        return new Object[][] {
                {
                        new int[] {0b10100001010001011010000101000101, 0b01010110100101101011010101010101, 0b01010010111110001111100010101010,
                                0b11111111000000011010101010101011, 0b00000000000000000000000000000000},
                        4,
                        0,
                        new int[] {0b10100001010001011010000101000101, 0b01010110100101101011010101010101, 0b01010010111110001111100010101010,
                                0b11111111000000011010101010101011, 0b00000000000000000000000000000000}
                },
                {
                        new int[] {0b00000000000000000000000000000000, 0b10100001010001011010000101000101, 0b01010110100101101011010101010101,
                                0b01010010111110001111100010101010, 0b11111111000000011010101010101011},
                        5,
                        1,
                        new int[] {0b10000000000000000000000000000000, 0b11010000101000101101000010100010, 0b00101011010010110101101010101010,
                                0b10101001011111000111110001010101, 0b1111111100000001101010101010101}
                },
                {
                        new int[] {0b00000000000000000000000000000000, 0b10100001010001011010000101000101, 0b01010110100101101011010101010101,
                                0b01010010111110001111100010101010, 0b11111111000000011010101010101011},
                        5,
                        32,
                        new int[] {0b10100001010001011010000101000101, 0b01010110100101101011010101010101, 0b01010010111110001111100010101010,
                                0b11111111000000011010101010101011, 0b00000000000000000000000000000000}
                },
                {
                        new int[] {0b00000000000000000000000000000000, 0b00000000000000000000000000000000, 0b10100001010001011010000101000101,
                                0b01010110100101101011010101010101, 0b01010010111110001111100010101010, 0b11111111000000011010101010101011},
                        6,
                        33,
                        new int[] {0b10000000000000000000000000000000, 0b11010000101000101101000010100010, 0b00101011010010110101101010101010,
                                0b10101001011111000111110001010101, 0b01111111100000001101010101010101, 0b00000000000000000000000000000000}
                },
                {
                        new int[] {0b00000000000000000000000000000000, 0b00000000000000000000000000000000, 0b10100001010001011010000101000101,
                                0b01010110100101101011010101010101, 0b01010010111110001111100010101010, 0b11111111000000011010101010101011},
                        6,
                        37,
                        new int[] {0b00101000000000000000000000000000, 0b10101101000010100010110100001010, 0b01010010101101001011010110101010,
                                0b01011010100101111100011111000101, 0b00000111111110000000110101010101, 0b00000000000000000000000000000000}
                },
                {
                        new int[] {0b00000000000000000000000000000000, 0b00000000000000000000000000000000, 0b10100001010001011010000101000101,
                                0b01010110100101101011010101010101, 0b01010010111110001111100010101010, 0b11111111000000011010101010101011},
                        6,
                        64,
                        new int[] {0b10100001010001011010000101000101, 0b01010110100101101011010101010101, 0b01010010111110001111100010101010,
                                0b11111111000000011010101010101011, 0b00000000000000000000000000000000, 0b00000000000000000000000000000000}
                },
        };
    }

    @Test(dataProvider = "testShiftRightMultiPrecisionCases")
    public void testShiftRightMultiPrecision(int[] number, int length, int shifts, int[] expected)
    {
        assertEquals(shiftRightMultiPrecision(number, length, shifts), expected);
    }

    @DataProvider
    public static Object[][] testShiftLeftCompareToBigIntegerCases()
    {
        return new Object[][] {
                {new BigInteger("446319580078125"), 19},

                {TWO.pow(1), 10},
                {TWO.pow(5).add(TWO.pow(1)), 10},
                {TWO.pow(1), 100},
                {TWO.pow(5).add(TWO.pow(1)), 100},

                {TWO.pow(70), 30},
                {TWO.pow(70).add(TWO.pow(1)), 30},

                {TWO.pow(106), 20},
                {TWO.pow(106).add(TWO.pow(1)), 20},
        };
    }

    @Test(dataProvider = "testShiftLeftCompareToBigIntegerCases")
    public void testShiftLeftCompareToBigInteger(BigInteger value, int leftShifts)
    {
        assertShiftLeft(value, leftShifts);
    }

    @DataProvider
    public static Object[][] testShiftLeftCompareToBigIntegerOverflowCases()
    {
        return new Object[][] {
                {TWO.pow(2), 127},
                {TWO.pow(2), 127},
                {TWO.pow(64), 64},
                {TWO.pow(100), 28},
        };
    }

    @Test(dataProvider = "testShiftLeftCompareToBigIntegerOverflowCases")
    public void testShiftLeftCompareToBigIntegerOverflow(BigInteger value, int leftShifts)
    {
        assertShiftLeftOverflow(value, leftShifts);
    }

    @DataProvider
    public static Object[][] testShiftLeftCases()
    {
        return new Object[][] {
                {new long[] {0x1234567890ABCDEFL, 0xEFDCBA0987654321L}, 0, new long[] {0x1234567890ABCDEFL, 0xEFDCBA0987654321L}},
                {new long[] {0x1234567890ABCDEFL, 0xEFDCBA0987654321L}, 1, new long[] {0x2468ACF121579BDEL, 0xDFB974130ECA8642L}},
                {new long[] {0x1234567890ABCDEFL, 0x00DCBA0987654321L}, 8, new long[] {0x34567890ABCDEF00L, 0xDCBA098765432112L}},
                {new long[] {0x1234567890ABCDEFL, 0x0000BA0987654321L}, 16, new long[] {0x567890ABCDEF0000L, 0xBA09876543211234L}},
                {new long[] {0x1234567890ABCDEFL, 0x0000000087654321L}, 32, new long[] {0x90ABCDEF00000000L, 0x8765432112345678L}},
                {new long[] {0x1234567890ABCDEFL, 0L}, 64, new long[] {0x0000000000000000L, 0x1234567890ABCDEFL}},
                {new long[] {0x0034567890ABCDEFL, 0L}, 64 + 8, new long[] {0x0000000000000000L, 0x34567890ABCDEF00L}},
                {new long[] {0x000000000000CDEFL, 0L}, 64 + 48, new long[] {0x0000000000000000L, 0xCDEF000000000000L}},
                {new long[] {0x1L, 0L}, 64 + 63, new long[] {0x0000000000000000L, 0x8000000000000000L}},
        };
    }

    @Test(dataProvider = "testShiftLeftCases")
    public void testShiftLeft(long[] value, int leftShifts, long[] expected)
    {
        assertEquals(shiftLeft(wrappedLongArray(value), leftShifts), wrappedLongArray(expected));
    }

    private void assertAddReturnOverflow(BigInteger left, BigInteger right)
    {
        Slice result = unscaledDecimal();
        long overflow = addWithOverflow(unscaledDecimal(left), unscaledDecimal(right), result);

        BigInteger actual = unscaledDecimalToBigInteger(result);
        BigInteger expected = left.add(right).remainder(TWO.pow(UnscaledDecimal128Arithmetic.UNSCALED_DECIMAL_128_SLICE_LENGTH * 8 - 1));
        BigInteger expectedOverflow = left.add(right).divide(TWO.pow(UnscaledDecimal128Arithmetic.UNSCALED_DECIMAL_128_SLICE_LENGTH * 8 - 1));

        assertEquals(actual, expected);
        assertEquals(overflow, expectedOverflow.longValueExact());
    }

    private static void assertUnscaledBigIntegerToDecimalOverflows(BigInteger value)
    {
        assertThatThrownBy(() -> unscaledDecimal(value))
                .isInstanceOf(ArithmeticException.class)
                .hasMessage("Decimal overflow");
    }

    private static void assertDecimalToUnscaledLongOverflows(BigInteger value)
    {
        Slice decimal = unscaledDecimal(value);
        assertThatThrownBy(() -> unscaledDecimalToUnscaledLong(decimal))
                .isInstanceOf(ArithmeticException.class)
                .hasMessage("Decimal overflow");
    }

    private static void assertMultiplyOverflows(Slice left, Slice right)
    {
        assertThatThrownBy(() -> multiply(left, right))
                .isInstanceOf(ArithmeticException.class)
                .hasMessage("Decimal overflow");
    }

    private static void assertRescaleOverflows(Slice decimal, int rescaleFactor)
    {
        assertThatThrownBy(() -> rescale(decimal, rescaleFactor))
                .isInstanceOf(ArithmeticException.class)
                .hasMessage("Decimal overflow");
    }

    private static void assertCompare(Slice left, Slice right, int expectedResult)
    {
        assertEquals(compare(left, right), expectedResult);
        assertEquals(compare(left.getLong(0), left.getLong(SIZE_OF_LONG), right.getLong(0), right.getLong(SIZE_OF_LONG)), expectedResult);
    }

    private static void assertConvertsUnscaledBigIntegerToDecimal(BigInteger value)
    {
        assertEquals(unscaledDecimalToBigInteger(unscaledDecimal(value)), value);
    }

    private static void assertConvertsUnscaledLongToDecimal(long value)
    {
        assertEquals(unscaledDecimalToUnscaledLong(unscaledDecimal(value)), value);
        assertEquals(unscaledDecimal(value), unscaledDecimal(BigInteger.valueOf(value)));
    }

    private static void assertShiftRight(Slice decimal, int rightShifts, boolean roundUp, Slice expectedResult)
    {
        Slice result = unscaledDecimal();
        shiftRight(decimal, rightShifts, roundUp, result);
        assertEquals(result, expectedResult);
    }

    private static void assertDivideAllSigns(int[] dividend, int[] divisor)
    {
        assertDivideAllSigns(Slices.wrappedIntArray(dividend), 0, Slices.wrappedIntArray(divisor), 0);
    }

    private void assertShiftLeftOverflow(BigInteger value, int leftShifts)
    {
        assertThatThrownBy(() -> assertShiftLeft(value, leftShifts))
                .isInstanceOf(ArithmeticException.class)
                .hasMessage("Decimal overflow");
    }

    private void assertShiftLeft(BigInteger value, int leftShifts)
    {
        Slice decimal = unscaledDecimal(value);
        BigInteger expectedResult = value.multiply(TWO.pow(leftShifts));
        shiftLeftDestructive(decimal, leftShifts);
        assertEquals(decodeUnscaledValue(decimal), expectedResult);
    }

    private static void assertDivideAllSigns(String dividend, String divisor)
    {
        assertDivideAllSigns(dividend, 0, divisor, 0);
    }

    private static void assertDivideAllSigns(String dividend, int dividendRescaleFactor, String divisor, int divisorRescaleFactor)
    {
        assertDivideAllSigns(unscaledDecimal(dividend), dividendRescaleFactor, unscaledDecimal(divisor), divisorRescaleFactor);
    }

    private static void assertDivideAllSigns(Slice dividend, int dividendRescaleFactor, Slice divisor, int divisorRescaleFactor)
    {
        assertDivide(dividend, dividendRescaleFactor, divisor, divisorRescaleFactor);
        if (!isZero(divisor)) {
            assertDivide(dividend, dividendRescaleFactor, negate(divisor), divisorRescaleFactor);
        }
        if (!isZero(dividend)) {
            assertDivide(negate(dividend), dividendRescaleFactor, divisor, divisorRescaleFactor);
        }
        if (!isZero(dividend) && !isZero(divisor)) {
            assertDivide(negate(dividend), dividendRescaleFactor, negate(divisor), divisorRescaleFactor);
        }
    }

    private static void assertDivide(Slice dividend, int dividendRescaleFactor, Slice divisor, int divisorRescaleFactor)
    {
        BigInteger dividendBigInteger = decodeUnscaledValue(dividend);
        BigInteger divisorBigInteger = decodeUnscaledValue(divisor);
        BigInteger rescaledDividend = dividendBigInteger.multiply(bigIntegerTenToNth(dividendRescaleFactor));
        BigInteger rescaledDivisor = divisorBigInteger.multiply(bigIntegerTenToNth(divisorRescaleFactor));
        BigInteger[] expectedQuotientAndRemainder = rescaledDividend.divideAndRemainder(rescaledDivisor);
        BigInteger expectedQuotient = expectedQuotientAndRemainder[0];
        BigInteger expectedRemainder = expectedQuotientAndRemainder[1];

        boolean overflowIsExpected = expectedQuotient.abs().compareTo(bigIntegerTenToNth(38)) >= 0 || expectedRemainder.abs().compareTo(bigIntegerTenToNth(38)) >= 0;

        Slice quotient = unscaledDecimal();
        Slice remainder = unscaledDecimal();
        try {
            divide(dividend, dividendRescaleFactor, divisor, divisorRescaleFactor, quotient, remainder);
            if (overflowIsExpected) {
                fail("overflow is expected");
            }
        }
        catch (ArithmeticException e) {
            if (!overflowIsExpected) {
                fail("overflow wasn't expected");
            }
            else {
                return;
            }
        }

        BigInteger actualQuotient = decodeUnscaledValue(quotient);
        BigInteger actualRemainder = decodeUnscaledValue(remainder);

        if (expectedQuotient.equals(actualQuotient) && expectedRemainder.equals(actualRemainder)) {
            return;
        }

        fail(format("%s / %s ([%s * 2^%d] / [%s * 2^%d]) Expected: %s(%s). Actual: %s(%s)",
                rescaledDividend, rescaledDivisor,
                dividendBigInteger, dividendRescaleFactor,
                divisorBigInteger, divisorRescaleFactor,
                expectedQuotient, expectedRemainder,
                actualQuotient, actualRemainder));
    }

    private static Slice negate(Slice slice)
    {
        Slice copy = unscaledDecimal(slice);
        UnscaledDecimal128Arithmetic.negate(copy);
        return copy;
    }

    private static boolean isShort(BigInteger value)
    {
        return value.abs().shiftRight(63).equals(BigInteger.ZERO);
    }

    private static void assertMultiply(BigInteger a, BigInteger b, BigInteger result)
    {
        assertEquals(unscaledDecimal(result), multiply(unscaledDecimal(a), unscaledDecimal(b)));
        if (isShort(a) && isShort(b)) {
            assertEquals(unscaledDecimal(result), multiply(a.longValue(), b.longValue()));
        }
        if (isShort(a) && !isShort(b)) {
            assertEquals(unscaledDecimal(result), multiply(unscaledDecimal(b), a.longValue()));
        }
        if (!isShort(a) && isShort(b)) {
            assertEquals(unscaledDecimal(result), multiply(unscaledDecimal(a), b.longValue()));
        }
    }

    private static void assertRescale(Slice decimal, int rescale, Slice expected)
    {
        assertEquals(rescale(decimal, rescale), expected);
        if (isShort(unscaledDecimalToBigInteger(decimal))) {
            Slice actual = rescale(unscaledDecimalToUnscaledLong(decimal), rescale);
            assertEquals(expected, actual);
        }
    }
}
