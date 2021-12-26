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

import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.Decimals.bigIntegerTenToNth;
import static io.trino.spi.type.Int128Math.add;
import static io.trino.spi.type.Int128Math.divideRoundUp;
import static io.trino.spi.type.Int128Math.multiply;
import static io.trino.spi.type.Int128Math.multiply256Destructive;
import static io.trino.spi.type.Int128Math.negate;
import static io.trino.spi.type.Int128Math.remainder;
import static io.trino.spi.type.Int128Math.rescale;
import static io.trino.spi.type.Int128Math.rescaleTruncate;
import static io.trino.spi.type.Int128Math.shiftLeftMultiPrecision;
import static io.trino.spi.type.Int128Math.shiftRight;
import static io.trino.spi.type.Int128Math.shiftRightMultiPrecision;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestInt128Math
{
    private static final Int128 MAX_DECIMAL = Int128.valueOf(Decimals.MAX_UNSCALED_DECIMAL.toBigInteger());
    private static final Int128 MIN_DECIMAL = Int128.valueOf(Decimals.MIN_UNSCALED_DECIMAL.toBigInteger());
    private static final BigInteger TWO = BigInteger.valueOf(2);

    @Test
    public void testUnscaledBigIntegerToDecimal()
    {
        assertConvertsUnscaledBigIntegerToDecimal(Decimals.MAX_UNSCALED_DECIMAL.toBigInteger());
        assertConvertsUnscaledBigIntegerToDecimal(Decimals.MIN_UNSCALED_DECIMAL.toBigInteger());
        assertConvertsUnscaledBigIntegerToDecimal(BigInteger.ZERO);
        assertConvertsUnscaledBigIntegerToDecimal(BigInteger.ONE);
        assertConvertsUnscaledBigIntegerToDecimal(BigInteger.ONE.negate());
    }

    @Test
    public void testUnscaledBigIntegerToInt128Overflow()
    {
        assertThatThrownBy(() -> Int128.valueOf(Int128.MAX_VALUE.toBigInteger().add(BigInteger.ONE)))
                .isInstanceOf(ArithmeticException.class)
                .hasMessage("BigInteger out of Int128 range");

        assertThatThrownBy(() -> Int128.valueOf(Int128.MIN_VALUE.toBigInteger().subtract(BigInteger.ONE)))
                .isInstanceOf(ArithmeticException.class)
                .hasMessage("BigInteger out of Int128 range");
    }

    @Test
    public void testUnscaledLongToDecimal()
    {
        assertConvertsUnscaledLongToDecimal(0);
        assertConvertsUnscaledLongToDecimal(1);
        assertConvertsUnscaledLongToDecimal(-1);
        assertConvertsUnscaledLongToDecimal(Long.MAX_VALUE);
        assertConvertsUnscaledLongToDecimal(Long.MIN_VALUE);
    }

    @Test
    public void testInt128ToUnscaledLongOverflow()
    {
        assertInt128ToLongOverflows(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE));
        assertInt128ToLongOverflows(BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE));
        assertInt128ToLongOverflows(Decimals.MAX_UNSCALED_DECIMAL.toBigInteger());
        assertInt128ToLongOverflows(Decimals.MIN_UNSCALED_DECIMAL.toBigInteger());
    }

    @Test
    public void testRescaleTruncate()
    {
        assertRescaleTruncate(Int128.valueOf(10), 0, Int128.valueOf(10L));
        assertRescaleTruncate(Int128.valueOf(-10), 0, Int128.valueOf(-10L));
        assertRescaleTruncate(Int128.valueOf(10), -20, Int128.valueOf(0L));
        assertRescaleTruncate(Int128.valueOf(14), -1, Int128.valueOf(1));
        assertRescaleTruncate(Int128.valueOf(14), -2, Int128.valueOf(0));
        assertRescaleTruncate(Int128.valueOf(14), -3, Int128.valueOf(0));
        assertRescaleTruncate(Int128.valueOf(15), -1, Int128.valueOf(1));
        assertRescaleTruncate(Int128.valueOf(15), -2, Int128.valueOf(0));
        assertRescaleTruncate(Int128.valueOf(15), -3, Int128.valueOf(0));
        assertRescaleTruncate(Int128.valueOf(1050), -3, Int128.valueOf(1));
        assertRescaleTruncate(Int128.valueOf(15), 1, Int128.valueOf(150));
        assertRescaleTruncate(Int128.valueOf(-14), -1, Int128.valueOf(-1));
        assertRescaleTruncate(Int128.valueOf(-14), -2, Int128.valueOf(0));
        assertRescaleTruncate(Int128.valueOf(-14), -20, Int128.valueOf(0));
        assertRescaleTruncate(Int128.valueOf(-15), -1, Int128.valueOf(-1));
        assertRescaleTruncate(Int128.valueOf(-15), -2, Int128.valueOf(0));
        assertRescaleTruncate(Int128.valueOf(-15), -20, Int128.valueOf(0));
        assertRescaleTruncate(Int128.valueOf(-14), 1, Int128.valueOf(-140));
        assertRescaleTruncate(Int128.valueOf(0), 1, Int128.valueOf(0));
        assertRescaleTruncate(Int128.valueOf(0), -1, Int128.valueOf(0));
        assertRescaleTruncate(Int128.valueOf(0), -20, Int128.valueOf(0));
        assertRescaleTruncate(Int128.valueOf(4), -1, Int128.valueOf(0));
        assertRescaleTruncate(Int128.valueOf(5), -1, Int128.valueOf(0));
        assertRescaleTruncate(Int128.valueOf(5), -2, Int128.valueOf(0));
        assertRescaleTruncate(Int128.valueOf(10), 10, Int128.valueOf(100000000000L));
        assertRescaleTruncate(Int128.valueOf("150000000000000000000"), -20, Int128.valueOf(1));
        assertRescaleTruncate(Int128.valueOf("-140000000000000000000"), -20, Int128.valueOf(-1));
        assertRescaleTruncate(Int128.valueOf("50000000000000000000"), -20, Int128.valueOf(0));
        assertRescaleTruncate(Int128.valueOf("150500000000000000000"), -18, Int128.valueOf(150));
        assertRescaleTruncate(Int128.valueOf("-140000000000000000000"), -18, Int128.valueOf(-140));
        assertRescaleTruncate(Int128.valueOf(BigInteger.ONE.shiftLeft(63)), -18, Int128.valueOf(9L));
        assertRescaleTruncate(Int128.valueOf(BigInteger.ONE.shiftLeft(62)), -18, Int128.valueOf(4L));
        assertRescaleTruncate(Int128.valueOf(BigInteger.ONE.shiftLeft(62)), -19, Int128.valueOf(0L));
        assertRescaleTruncate(MAX_DECIMAL, -1, Int128.valueOf("9999999999999999999999999999999999999"));
        assertRescaleTruncate(MIN_DECIMAL, -10, Int128.valueOf("-9999999999999999999999999999"));
        assertRescaleTruncate(Int128.valueOf(1), 37, Int128.valueOf("10000000000000000000000000000000000000"));
        assertRescaleTruncate(Int128.valueOf(-1), 37, Int128.valueOf("-10000000000000000000000000000000000000"));
        assertRescaleTruncate(Int128.valueOf("10000000000000000000000000000000000000"), -37, Int128.valueOf(1));
    }

    @Test
    public void testRescale()
    {
        assertRescale(Int128.valueOf(10), 0, Int128.valueOf(10L));
        assertRescale(Int128.valueOf(-10), 0, Int128.valueOf(-10L));
        assertRescale(Int128.valueOf(10), -20, Int128.valueOf(0L));
        assertRescale(Int128.valueOf(14), -1, Int128.valueOf(1));
        assertRescale(Int128.valueOf(14), -2, Int128.valueOf(0));
        assertRescale(Int128.valueOf(14), -3, Int128.valueOf(0));
        assertRescale(Int128.valueOf(15), -1, Int128.valueOf(2));
        assertRescale(Int128.valueOf(15), -2, Int128.valueOf(0));
        assertRescale(Int128.valueOf(15), -3, Int128.valueOf(0));
        assertRescale(Int128.valueOf(1050), -3, Int128.valueOf(1));
        assertRescale(Int128.valueOf(15), 1, Int128.valueOf(150));
        assertRescale(Int128.valueOf(-14), -1, Int128.valueOf(-1));
        assertRescale(Int128.valueOf(-14), -2, Int128.valueOf(0));
        assertRescale(Int128.valueOf(-14), -20, Int128.valueOf(0));
        assertRescale(Int128.valueOf(-15), -1, Int128.valueOf(-2));
        assertRescale(Int128.valueOf(-15), -2, Int128.valueOf(0));
        assertRescale(Int128.valueOf(-15), -20, Int128.valueOf(0));
        assertRescale(Int128.valueOf(-14), 1, Int128.valueOf(-140));
        assertRescale(Int128.valueOf(0), 1, Int128.valueOf(0));
        assertRescale(Int128.valueOf(0), -1, Int128.valueOf(0));
        assertRescale(Int128.valueOf(0), -20, Int128.valueOf(0));
        assertRescale(Int128.valueOf(4), -1, Int128.valueOf(0));
        assertRescale(Int128.valueOf(5), -1, Int128.valueOf(1));
        assertRescale(Int128.valueOf(5), -2, Int128.valueOf(0));
        assertRescale(Int128.valueOf(10), 10, Int128.valueOf(100000000000L));
        assertRescale(Int128.valueOf("150000000000000000000"), -20, Int128.valueOf(2));
        assertRescale(Int128.valueOf("-140000000000000000000"), -20, Int128.valueOf(-1));
        assertRescale(Int128.valueOf("50000000000000000000"), -20, Int128.valueOf(1));
        assertRescale(Int128.valueOf("150500000000000000000"), -18, Int128.valueOf(151));
        assertRescale(Int128.valueOf("-140000000000000000000"), -18, Int128.valueOf(-140));
        assertRescale(Int128.valueOf(BigInteger.ONE.shiftLeft(63)), -18, Int128.valueOf(9L));
        assertRescale(Int128.valueOf(BigInteger.ONE.shiftLeft(62)), -18, Int128.valueOf(5L));
        assertRescale(Int128.valueOf(BigInteger.ONE.shiftLeft(62)), -19, Int128.valueOf(0L));
        assertRescale(MAX_DECIMAL, -1, Int128.valueOf(Decimals.MAX_UNSCALED_DECIMAL.toBigInteger().divide(BigInteger.TEN).add(BigInteger.ONE)));
        assertRescale(MIN_DECIMAL, -10, Int128.valueOf(Decimals.MIN_UNSCALED_DECIMAL.toBigInteger().divide(BigInteger.valueOf(10000000000L)).subtract(BigInteger.ONE)));
        assertRescale(Int128.valueOf(1), 37, Int128.valueOf("10000000000000000000000000000000000000"));
        assertRescale(Int128.valueOf(-1), 37, Int128.valueOf("-10000000000000000000000000000000000000"));
        assertRescale(Int128.valueOf("10000000000000000000000000000000000000"), -37, Int128.valueOf(1));
    }

    @Test
    public void testRescaleOverflows()
    {
        assertRescaleOverflows(Int128.valueOf(1), 38);
    }

    @Test
    public void testAdd()
    {
        assertAdd(Int128.valueOf(0), Int128.valueOf(0), Int128.valueOf(0));
        assertAdd(Int128.valueOf(1), Int128.valueOf(0), Int128.valueOf(1));
        assertAdd(Int128.valueOf(1), Int128.valueOf(1), Int128.valueOf(2));
        assertAdd(Int128.valueOf(-1), Int128.valueOf(0), Int128.valueOf(-1));
        assertAdd(Int128.valueOf(-1), Int128.valueOf(-1), Int128.valueOf(-2));
        assertAdd(Int128.valueOf(-1), Int128.valueOf(1), Int128.valueOf(0));
        assertAdd(Int128.valueOf(1), Int128.valueOf(-1), Int128.valueOf(0));
        assertAdd(Int128.valueOf("10000000000000000000000000000000000000"), Int128.valueOf(0), Int128.valueOf("10000000000000000000000000000000000000"));
        assertAdd(Int128.valueOf("10000000000000000000000000000000000000"), Int128.valueOf("10000000000000000000000000000000000000"), Int128.valueOf("20000000000000000000000000000000000000"));
        assertAdd(Int128.valueOf("-10000000000000000000000000000000000000"), Int128.valueOf(0), Int128.valueOf("-10000000000000000000000000000000000000"));
        assertAdd(Int128.valueOf("-10000000000000000000000000000000000000"), Int128.valueOf("-10000000000000000000000000000000000000"), Int128.valueOf("-20000000000000000000000000000000000000"));
        assertAdd(Int128.valueOf("-10000000000000000000000000000000000000"), Int128.valueOf("10000000000000000000000000000000000000"), Int128.valueOf(0));
        assertAdd(Int128.valueOf("10000000000000000000000000000000000000"), Int128.valueOf("-10000000000000000000000000000000000000"), Int128.valueOf(0));

        assertAdd(Int128.valueOf(1L << 32), Int128.valueOf(0), Int128.valueOf(1L << 32));
        assertAdd(Int128.valueOf(1L << 31), Int128.valueOf(1L << 31), Int128.valueOf(1L << 32));
        assertAdd(Int128.valueOf(1L << 32), Int128.valueOf(1L << 33), Int128.valueOf((1L << 32) + (1L << 33)));
    }

    @Test
    public void testMultiply()
    {
        assertMultiply(0, 0, 0);
        assertMultiply(1, 0, 0);
        assertMultiply(0, 1, 0);
        assertMultiply(-1, 0, 0);
        assertMultiply(0, -1, 0);
        assertMultiply(1, 1, 1);
        assertMultiply(1, -1, -1);
        assertMultiply(-1, -1, 1);
        assertMultiply(Decimals.MAX_UNSCALED_DECIMAL.toBigInteger(), 0, 0);
        assertMultiply(Decimals.MAX_UNSCALED_DECIMAL.toBigInteger(), 1, Decimals.MAX_UNSCALED_DECIMAL.toBigInteger());
        assertMultiply(Decimals.MIN_UNSCALED_DECIMAL.toBigInteger(), 0, 0);
        assertMultiply(Decimals.MIN_UNSCALED_DECIMAL.toBigInteger(), 1, Decimals.MIN_UNSCALED_DECIMAL.toBigInteger());
        assertMultiply(Decimals.MAX_UNSCALED_DECIMAL.toBigInteger(), -1, Decimals.MIN_UNSCALED_DECIMAL.toBigInteger());
        assertMultiply(Decimals.MIN_UNSCALED_DECIMAL.toBigInteger(), -1, Decimals.MAX_UNSCALED_DECIMAL.toBigInteger());
        assertMultiply(new BigInteger("FFFFFFFFFFFFFFFF", 16), new BigInteger("FFFFFFFFFFFFFF", 16), new BigInteger("fffffffffffffeff00000000000001", 16));
        assertMultiply(new BigInteger("FFFFFF0096BFB800", 16), new BigInteger("39003539D9A51600", 16), new BigInteger("39003500FB00AB761CDBB17E11D00000", 16));
        assertMultiply(Integer.MAX_VALUE, Integer.MIN_VALUE, (long) Integer.MAX_VALUE * Integer.MIN_VALUE);
        assertMultiply(new BigInteger("99999999999999"), new BigInteger("-1000000000000000000000000"), new BigInteger("-99999999999999000000000000000000000000"));
        assertMultiply(new BigInteger("12380837221737387489365741632769922889"), 3, new BigInteger("37142511665212162468097224898309768667"));
    }

    @Test
    public void testMultiply256()
    {
        assertMultiply256(
                MAX_DECIMAL,
                MAX_DECIMAL,
                new int[] {0x00000001, 0xECEBBB80, 0xBC87870B, 0xE0FF0CA0, 0xE8652978, 0x0764B4AB, 0x119915B5, 0x161BCCA7});
        assertMultiply256(
                Int128.valueOf(0x0FFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL),
                Int128.valueOf(0x0FFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL),
                new int[] {0x00000001, 0x00000000, 0x00000000, 0xE0000000, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0x00FFFFFF});
        assertMultiply256(
                Int128.valueOf(0x0EDCBA0987654321L, 0x1234567890ABCDEFL),
                Int128.valueOf(0x1234567890ABCDEL, 0xFEDCBA0987654321L),
                new int[] {0xE55618CF, 0xC24A442F, 0x0DA49DDA, 0xAA71A60D, 0x13DF8695, 0x7C163D5A, 0xF9BD1294, 0x0010E8EE});
    }

    private static void assertMultiply256(Int128 left, Int128 right, int[] expected)
    {
        int[] leftArg = new int[8];
        leftArg[0] = (int) left.getLow();
        leftArg[1] = (int) (left.getLow() >> 32);
        leftArg[2] = (int) left.getHigh();
        leftArg[3] = (int) (left.getHigh() >> 32);

        multiply256Destructive(leftArg, right);
        assertEquals(leftArg, expected);
    }

    @Test
    public void testMultiplyOverflow()
    {
        assertMultiplyOverflows(Int128.valueOf("99999999999999"), Int128.valueOf("-10000000000000000000000000"));
        assertMultiplyOverflows(MAX_DECIMAL, Int128.valueOf("10"));

        assertMultiplyOverflows(Int128.valueOf("18446744073709551616"), Int128.valueOf("18446744073709551616")); // 2^64 * 2^64
        assertMultiplyOverflows(Int128.valueOf("18446744073709551615"), Int128.valueOf("18446744073709551615")); // (2^64 - 1) * (2^64 - 1)
        assertMultiplyOverflows(Int128.valueOf("85070591730234615865843651857942052864"), Int128.valueOf("2")); // 2^126 * 2
        assertMultiplyOverflows(Int128.valueOf("2"), Int128.valueOf("85070591730234615865843651857942052864")); // 2 * 2^126

        assertMultiplyOverflows(Int128.valueOf("-18446744073709551616"), Int128.valueOf("18446744073709551616")); // -(2^64) * 2^64
        assertMultiplyOverflows(Int128.valueOf("-18446744073709551615"), Int128.valueOf("18446744073709551615")); // 1(2^64 - 1) * (2^64 - 1)
        assertMultiplyOverflows(Int128.valueOf("85070591730234615865843651857942052864"), Int128.valueOf("-2")); // 2^126 * -2
        assertMultiplyOverflows(Int128.valueOf("-2"), Int128.valueOf("85070591730234615865843651857942052864")); // -2 * 2^126
    }

    @Test
    public void testShiftRight()
    {
        assertShiftRight(Int128.valueOf(0), 0, true, Int128.valueOf(0));
        assertShiftRight(Int128.valueOf(0), 33, true, Int128.valueOf(0));

        assertShiftRight(Int128.valueOf(1), 1, true, Int128.valueOf(1));
        assertShiftRight(Int128.valueOf(1), 1, false, Int128.valueOf(0));
        assertShiftRight(Int128.valueOf(1), 2, true, Int128.valueOf(0));
        assertShiftRight(Int128.valueOf(1), 2, false, Int128.valueOf(0));

        assertShiftRight(Int128.valueOf(1L << 32), 32, true, Int128.valueOf(1));
        assertShiftRight(Int128.valueOf(1L << 31), 32, true, Int128.valueOf(1));
        assertShiftRight(Int128.valueOf(1L << 31), 32, false, Int128.valueOf(0));
        assertShiftRight(Int128.valueOf(3L << 33), 34, true, Int128.valueOf(2));
        assertShiftRight(Int128.valueOf(3L << 33), 34, false, Int128.valueOf(1));
        assertShiftRight(Int128.valueOf(BigInteger.valueOf(0x7FFFFFFFFFFFFFFFL).setBit(63).setBit(64)), 1, true, Int128.valueOf(BigInteger.ONE.shiftLeft(64)));

        assertShiftRight(MAX_DECIMAL, 1, true, Int128.valueOf(MAX_DECIMAL.toBigInteger().shiftRight(1).add(BigInteger.ONE)));
        assertShiftRight(MAX_DECIMAL, 66, true, Int128.valueOf(MAX_DECIMAL.toBigInteger().shiftRight(66).add(BigInteger.ONE)));
    }

    @Test
    public void testDivide()
    {
        // simple cases
        assertDivideAllSigns("0", "10");
        assertDivideAllSigns("5", "10");
        assertDivideAllSigns("50", "100");
        assertDivideAllSigns("99", "10");
        assertDivideAllSigns("95", "10");
        assertDivideAllSigns("91", "10");
        assertDivideAllSigns("1000000000000000000000000", "10");
        assertDivideAllSigns("1000000000000000000000000", "3");
        assertDivideAllSigns("1000000000000000000000000", "9");
        assertDivideAllSigns("1000000000000000000000000", "100000000000000000000000");
        assertDivideAllSigns("1000000000000000000000000", "333333333333333333333333");
        assertDivideAllSigns("1000000000000000000000000", "111111111111111111111111");

        // dividend < divisor
        assertDivideAllSigns(new int[] {4, 3, 2, 0}, new int[] {4, 3, 2, 1});
        assertDivideAllSigns(new int[] {4, 3, 0, 0}, new int[] {4, 3, 2, 0});
        assertDivideAllSigns(new int[] {4, 0, 0, 0}, new int[] {4, 3, 0, 0});
        assertDivideAllSigns(new int[] {0, 0, 0, 0}, new int[] {4, 0, 0, 0});

        // different lengths
        assertDivideAllSigns(new int[] {1423957378, 1765820914, 0xFFFFFFFF, 0}, new int[] {4, 0x0000FFFF, 0, 0});
        assertDivideAllSigns(new int[] {1423957378, 1765820914, 0xFFFFFFFF, 0}, new int[] {2042457708, 0, 0, 0});
        assertDivideAllSigns(new int[] {1423957378, -925263858, 0, 0}, new int[] {2042457708, 0, 0, 0});
        assertDivideAllSigns(new int[] {0xFFFFFFFF, 0, 0, 0}, new int[] {2042457708, 0, 0, 0});

        // single int divisor
        assertDivideAllSigns(new int[] {1423957378, -1444436990, -925263858, 1106345725}, new int[] {2042457708, 0, 0, 0});
        assertDivideAllSigns(new int[] {0, 0xF7000000, 0, 0x39000000}, new int[] {-1765820914, 0, 0, 0});

        // normalization scale = 1
        assertDivideAllSigns(new int[] {0x0FF00210, 0xF7001230, 0xFB00AC00, 0x39003500}, new int[] {-1765820914, 2042457708, 0xFFFFFFFF, 0});
        assertDivideAllSigns(new int[] {0x0FF00210, 0xF7001230, 0xFB00AC00, 0x39003500}, new int[] {-1765820914, 0xFFFFFF00, 0, 0});
        assertDivideAllSigns(new int[] {0x0FF00210, 0xF7001230, 0xFB00AC00, 0x39003500}, new int[] {-1765820914, 0xFF000000, 0, 0});

        // normalization scale > 1
        assertDivideAllSigns(new int[] {0x0FF00210, 0xF7001230, 0xFB00AC00, 0x39003500}, new int[] {-1765820914, 2042457708, 0xFFFFFFFF, 0x7FFFFFFF});
        assertDivideAllSigns(new int[] {0x0FF00210, 0xF7001230, 0xFB00AC00, 0x39003500}, new int[] {-1765820914, 2042457708, 0x4FFFFFFF, 0});
        assertDivideAllSigns(new int[] {0x0FF00210, 0xF7001230, 0xFB00AC00, 0x39003500}, new int[] {-1765820914, 2042457708, 0x0000FFFF, 0});

        // normalization scale signed overflow
        assertDivideAllSigns(new int[] {1, 1, 1, 0x7FFFFFFF}, new int[] {0xFFFFFFFF, 1, 0, 0});

        // u2 = v1
        assertDivideAllSigns(new int[] {0, 0x8FFFFFFF, 0x8FFFFFFF, 0}, new int[] {0xFFFFFFFF, 0x8FFFFFFF, 0, 0});

        // qhat is greater than q by 1
        assertDivideAllSigns(new int[] {1, 1, 0xFFFFFFFF, 0}, new int[] {0xFFFFFFFF, 0x7FFFFFFF, 0, 0});

        // qhat is greater than q by 2
        assertDivideAllSigns(new int[] {1, 1, 0xFFFFFFFF, 0}, new int[] {0xFFFFFFFF, 0x7FFFFFFF, 0, 0});

        // overflow after multiplyAndSubtract
        assertDivideAllSigns(new int[] {0x00000003, 0x00000000, 0x80000000, 0}, new int[] {0x00000001, 0x00000000, 0x20000000, 0});
        assertDivideAllSigns(new int[] {0x00000003, 0x00000000, 0x00008000, 0}, new int[] {0x00000001, 0x00000000, 0x00002000, 0});
        assertDivideAllSigns(new int[] {0, 0, 0x00008000, 0x00007fff}, new int[] {1, 0, 0x00008000, 0});

        // test cases from http://www.hackersdelight.org/hdcodetxt/divmnu64.c.txt
        // license: http://www.hackersdelight.org/permissions.htm
        assertDivideAllSigns(new int[] {3, 0, 0, 0}, new int[] {2, 0, 0, 0});
        assertDivideAllSigns(new int[] {3, 0, 0, 0}, new int[] {3, 0, 0, 0});
        assertDivideAllSigns(new int[] {3, 0, 0, 0}, new int[] {4, 0, 0, 0});
        assertDivideAllSigns(new int[] {3, 0, 0, 0}, new int[] {0xffffffff, 0, 0, 0});
        assertDivideAllSigns(new int[] {0xffffffff, 0, 0, 0}, new int[] {1, 0, 0, 0});
        assertDivideAllSigns(new int[] {0xffffffff, 0, 0, 0}, new int[] {0xffffffff, 0, 0, 0});
        assertDivideAllSigns(new int[] {0xffffffff, 0, 0, 0}, new int[] {3, 0, 0, 0});
        assertDivideAllSigns(new int[] {0xffffffff, 0xffffffff, 0, 0}, new int[] {1, 0, 0, 0});
        assertDivideAllSigns(new int[] {0xffffffff, 0xffffffff, 0, 0}, new int[] {0xffffffff, 0, 0, 0});
        assertDivideAllSigns(new int[] {0xffffffff, 0xfffffffe, 0, 0}, new int[] {0xffffffff, 0, 0, 0});
        assertDivideAllSigns(new int[] {0x00005678, 0x00001234, 0, 0}, new int[] {0x00009abc, 0, 0, 0});
        assertDivideAllSigns(new int[] {0, 0, 0, 0}, new int[] {0, 1, 0, 0});
        assertDivideAllSigns(new int[] {0, 7, 0, 0}, new int[] {0, 3, 0, 0});
        assertDivideAllSigns(new int[] {5, 7, 0, 0}, new int[] {0, 3, 0, 0});
        assertDivideAllSigns(new int[] {0, 6, 0, 0}, new int[] {0, 2, 0, 0});
        assertDivideAllSigns(new int[] {0x00000000, 0x80000000, 0, 0}, new int[] {0x40000001, 0, 0, 0});
        assertDivideAllSigns(new int[] {0x00000000, 0x80000000, 0, 0}, new int[] {0x00000001, 0x40000000, 0, 0});
        assertDivideAllSigns(new int[] {0x0000789a, 0x0000bcde, 0, 0}, new int[] {0x0000789a, 0x0000bcde, 0, 0});
        assertDivideAllSigns(new int[] {0x0000789b, 0x0000bcde, 0, 0}, new int[] {0x0000789a, 0x0000bcde, 0, 0});
        assertDivideAllSigns(new int[] {0x00007899, 0x0000bcde, 0, 0}, new int[] {0x0000789a, 0x0000bcde, 0, 0});
        assertDivideAllSigns(new int[] {0x0000ffff, 0x0000ffff, 0, 0}, new int[] {0x0000ffff, 0x0000ffff, 0, 0});
        assertDivideAllSigns(new int[] {0x0000ffff, 0x0000ffff, 0, 0}, new int[] {0x00000000, 0x0000ffff, 0, 0});
        assertDivideAllSigns(new int[] {0x000089ab, 0x00004567, 0x00000123, 0}, new int[] {0x00000000, 0x00000001, 0, 0});
        assertDivideAllSigns(new int[] {0x000089ab, 0x00004567, 0x00000123, 0}, new int[] {0x00000000, 0x00000001, 0, 0});
        assertDivideAllSigns(new int[] {0x00000000, 0x0000fffe, 0x00008000, 0}, new int[] {0x0000ffff, 0x00008000, 0, 0});
        assertDivideAllSigns(new int[] {0x00000003, 0x00000000, 0x80000000, 0}, new int[] {0x00000001, 0x00000000, 0x20000000, 0});
        assertDivideAllSigns(new int[] {0x00000003, 0x00000000, 0x00008000, 0}, new int[] {0x00000001, 0x00000000, 0x00002000, 0});
        assertDivideAllSigns(new int[] {0, 0, 0x00008000, 0x00007fff}, new int[] {1, 0, 0x00008000, 0});
        assertDivideAllSigns(new int[] {0, 0x0000fffe, 0, 0x00008000}, new int[] {0x0000ffff, 0, 0x00008000, 0});
        assertDivideAllSigns(new int[] {0, 0xfffffffe, 0, 0x80000000}, new int[] {0x0000ffff, 0, 0x80000000, 0});
        assertDivideAllSigns(new int[] {0, 0xfffffffe, 0, 0x80000000}, new int[] {0xffffffff, 0, 0x80000000, 0});

        // with rescale
        assertDivideAllSigns("100000000000000000000000", 10, "111111111111111111111111", 10);
        assertDivideAllSigns("100000000000000000000000", 10, "111111111111", 22);
        assertDivideAllSigns("99999999999999999999999999999999999999", 37, "99999999999999999999999999999999999999", 37);
        assertDivideAllSigns("99999999999999999999999999999999999999", 2, "99999999999999999999999999999999999999", 1);
        assertDivideAllSigns("99999999999999999999999999999999999999", 37, "9", 37);
        assertDivideAllSigns("99999999999999999999999999999999999999", 37, "1", 37);
        assertDivideAllSigns("11111111111111111111111111111111111111", 37, "2", 37);
        assertDivideAllSigns("11111111111111111111111111111111111111", 37, "2", 1);
        assertDivideAllSigns("97764425639372288753711864842425458618", 36, "32039006229599111733094986468789901155", 0);
        assertDivideAllSigns("34354576602352622842481633786816220283", 0, "31137583115118564930544829855652258045", 0);
        assertDivideAllSigns("96690614752287690630596513604374991473", 0, "10039352042372909488692220528497751229", 0);
        assertDivideAllSigns("87568357716090115374029040878755891076", 0, "46106713604991337798209343815577148589", 0);
    }

    @Test
    public void testCompare()
    {
        assertCompare(Int128.valueOf(0), Int128.valueOf(0), 0);

        assertCompare(Int128.valueOf(0), Int128.valueOf(10), -1);
        assertCompare(Int128.valueOf(10), Int128.valueOf(0), 1);

        assertCompare(Int128.valueOf(-10), Int128.valueOf(-11), 1);
        assertCompare(Int128.valueOf(-11), Int128.valueOf(-11), 0);
        assertCompare(Int128.valueOf(-12), Int128.valueOf(-11), -1);

        assertCompare(Int128.valueOf(10), Int128.valueOf(11), -1);
        assertCompare(Int128.valueOf(11), Int128.valueOf(11), 0);
        assertCompare(Int128.valueOf(12), Int128.valueOf(11), 1);
    }

    @Test
    public void testNegate()
    {
        assertEquals(negate(negate(MIN_DECIMAL)), MIN_DECIMAL);
        assertEquals(negate(MIN_DECIMAL), MAX_DECIMAL);
        assertEquals(negate(MIN_DECIMAL), MAX_DECIMAL);

        assertEquals(negate(Int128.valueOf(1)), Int128.valueOf(-1));
        assertEquals(negate(Int128.valueOf(-1)), Int128.valueOf(1));
    }

    @Test
    public void testIsNegative()
    {
        assertTrue(MIN_DECIMAL.isNegative());
        assertFalse(MAX_DECIMAL.isNegative());
        assertFalse(Int128.ZERO.isNegative());
    }

    @Test
    public void testToString()
    {
        assertEquals(Int128.ZERO.toString(), "0");
        assertEquals(Int128.valueOf(1).toString(), "1");
        assertEquals(Int128.valueOf(-1).toString(), "-1");
        assertEquals(MAX_DECIMAL.toString(), Decimals.MAX_UNSCALED_DECIMAL.toBigInteger().toString());
        assertEquals(MIN_DECIMAL.toString(), Decimals.MIN_UNSCALED_DECIMAL.toBigInteger().toString());
        assertEquals(Int128.valueOf("1000000000000000000000000000000000000").toString(), "1000000000000000000000000000000000000");
        assertEquals(Int128.valueOf("-1000000000002000000000000300000000000").toString(), "-1000000000002000000000000300000000000");
    }

    @Test
    public void testShiftLeftMultiPrecision()
    {
        assertEquals(shiftLeftMultiPrecision(
                        new int[] {0b10100001010001011010000101000101, 0b01010110100101101011010101010101, 0b01010010111110001111100010101010,
                                0b11111111000000011010101010101011, 0b00000000000000000000000000000000}, 4, 0),
                new int[] {0b10100001010001011010000101000101, 0b01010110100101101011010101010101, 0b01010010111110001111100010101010,
                        0b11111111000000011010101010101011, 0b00000000000000000000000000000000});
        assertEquals(shiftLeftMultiPrecision(
                        new int[] {0b10100001010001011010000101000101, 0b01010110100101101011010101010101, 0b01010010111110001111100010101010,
                                0b11111111000000011010101010101011, 0b00000000000000000000000000000000}, 5, 1),
                new int[] {0b01000010100010110100001010001010, 0b10101101001011010110101010101011, 0b10100101111100011111000101010100,
                        0b11111110000000110101010101010110, 0b00000000000000000000000000000001});
        assertEquals(shiftLeftMultiPrecision(
                        new int[] {0b10100001010001011010000101000101, 0b01010110100101101011010101010101, 0b01010010111110001111100010101010,
                                0b11111111000000011010101010101011, 0b00000000000000000000000000000000}, 5, 31),
                new int[] {0b10000000000000000000000000000000, 0b11010000101000101101000010100010, 0b00101011010010110101101010101010,
                        0b10101001011111000111110001010101, 0b1111111100000001101010101010101});
        assertEquals(shiftLeftMultiPrecision(
                        new int[] {0b10100001010001011010000101000101, 0b01010110100101101011010101010101, 0b01010010111110001111100010101010,
                                0b11111111000000011010101010101011, 0b00000000000000000000000000000000}, 5, 32),
                new int[] {0b00000000000000000000000000000000, 0b10100001010001011010000101000101, 0b01010110100101101011010101010101,
                        0b01010010111110001111100010101010, 0b11111111000000011010101010101011});
        assertEquals(shiftLeftMultiPrecision(
                        new int[] {0b10100001010001011010000101000101, 0b01010110100101101011010101010101, 0b01010010111110001111100010101010,
                                0b11111111000000011010101010101011, 0b00000000000000000000000000000000, 0b00000000000000000000000000000000}, 6, 33),
                new int[] {0b00000000000000000000000000000000, 0b01000010100010110100001010001010, 0b10101101001011010110101010101011,
                        0b10100101111100011111000101010100, 0b11111110000000110101010101010110, 0b00000000000000000000000000000001});
        assertEquals(shiftLeftMultiPrecision(
                        new int[] {0b10100001010001011010000101000101, 0b01010110100101101011010101010101, 0b01010010111110001111100010101010,
                                0b11111111000000011010101010101011, 0b00000000000000000000000000000000, 0b00000000000000000000000000000000}, 6, 37),
                new int[] {0b00000000000000000000000000000000, 0b00101000101101000010100010100000, 0b11010010110101101010101010110100,
                        0b01011111000111110001010101001010, 0b11100000001101010101010101101010, 0b00000000000000000000000000011111});
        assertEquals(shiftLeftMultiPrecision(
                        new int[] {0b10100001010001011010000101000101, 0b01010110100101101011010101010101, 0b01010010111110001111100010101010,
                                0b11111111000000011010101010101011, 0b00000000000000000000000000000000, 0b00000000000000000000000000000000}, 6, 64),
                new int[] {0b00000000000000000000000000000000, 0b00000000000000000000000000000000, 0b10100001010001011010000101000101,
                        0b01010110100101101011010101010101, 0b01010010111110001111100010101010, 0b11111111000000011010101010101011});
    }

    @Test
    public void testShiftRightMultiPrecision()
    {
        assertEquals(shiftRightMultiPrecision(
                        new int[] {
                                0b10100001010001011010000101000101, 0b01010110100101101011010101010101, 0b01010010111110001111100010101010,
                                0b11111111000000011010101010101011, 0b00000000000000000000000000000000}, 4, 0),
                new int[] {
                        0b10100001010001011010000101000101, 0b01010110100101101011010101010101, 0b01010010111110001111100010101010,
                        0b11111111000000011010101010101011, 0b00000000000000000000000000000000});
        assertEquals(shiftRightMultiPrecision(
                        new int[] {
                                0b00000000000000000000000000000000, 0b10100001010001011010000101000101, 0b01010110100101101011010101010101,
                                0b01010010111110001111100010101010, 0b11111111000000011010101010101011}, 5, 1),
                new int[] {
                        0b10000000000000000000000000000000, 0b11010000101000101101000010100010, 0b00101011010010110101101010101010,
                        0b10101001011111000111110001010101, 0b1111111100000001101010101010101});
        assertEquals(shiftRightMultiPrecision(
                        new int[] {
                                0b00000000000000000000000000000000, 0b10100001010001011010000101000101, 0b01010110100101101011010101010101,
                                0b01010010111110001111100010101010, 0b11111111000000011010101010101011}, 5, 32),
                new int[] {
                        0b10100001010001011010000101000101, 0b01010110100101101011010101010101, 0b01010010111110001111100010101010,
                        0b11111111000000011010101010101011, 0b00000000000000000000000000000000});
        assertEquals(shiftRightMultiPrecision(
                        new int[] {
                                0b00000000000000000000000000000000, 0b00000000000000000000000000000000, 0b10100001010001011010000101000101,
                                0b01010110100101101011010101010101, 0b01010010111110001111100010101010, 0b11111111000000011010101010101011}, 6, 33),
                new int[] {
                        0b10000000000000000000000000000000, 0b11010000101000101101000010100010, 0b00101011010010110101101010101010,
                        0b10101001011111000111110001010101, 0b01111111100000001101010101010101, 0b00000000000000000000000000000000});
        assertEquals(shiftRightMultiPrecision(
                        new int[] {
                                0b00000000000000000000000000000000, 0b00000000000000000000000000000000, 0b10100001010001011010000101000101,
                                0b01010110100101101011010101010101, 0b01010010111110001111100010101010, 0b11111111000000011010101010101011}, 6, 37),
                new int[] {
                        0b00101000000000000000000000000000, 0b10101101000010100010110100001010, 0b01010010101101001011010110101010,
                        0b01011010100101111100011111000101, 0b00000111111110000000110101010101, 0b00000000000000000000000000000000});
        assertEquals(shiftRightMultiPrecision(
                        new int[] {
                                0b00000000000000000000000000000000, 0b00000000000000000000000000000000, 0b10100001010001011010000101000101,
                                0b01010110100101101011010101010101, 0b01010010111110001111100010101010, 0b11111111000000011010101010101011}, 6, 64),
                new int[] {
                        0b10100001010001011010000101000101, 0b01010110100101101011010101010101, 0b01010010111110001111100010101010,
                        0b11111111000000011010101010101011, 0b00000000000000000000000000000000, 0b00000000000000000000000000000000});
    }

    @Test
    public void testShiftLeft()
    {
        assertEquals(shiftLeft(Int128.valueOf(0xEFDCBA0987654321L, 0x1234567890ABCDEFL), 0), Int128.valueOf(0xEFDCBA0987654321L, 0x1234567890ABCDEFL));
        assertEquals(shiftLeft(Int128.valueOf(0xEFDCBA0987654321L, 0x1234567890ABCDEFL), 1), Int128.valueOf(0xDFB974130ECA8642L, 0x2468ACF121579BDEL));
        assertEquals(shiftLeft(Int128.valueOf(0x00DCBA0987654321L, 0x1234567890ABCDEFL), 8), Int128.valueOf(0xDCBA098765432112L, 0x34567890ABCDEF00L));
        assertEquals(shiftLeft(Int128.valueOf(0x0000BA0987654321L, 0x1234567890ABCDEFL), 16), Int128.valueOf(0xBA09876543211234L, 0x567890ABCDEF0000L));
        assertEquals(shiftLeft(Int128.valueOf(0x0000000087654321L, 0x1234567890ABCDEFL), 32), Int128.valueOf(0x8765432112345678L, 0x90ABCDEF00000000L));
        assertEquals(shiftLeft(Int128.valueOf(0L, 0x1234567890ABCDEFL), 64), Int128.valueOf(0x1234567890ABCDEFL, 0x0000000000000000L));
        assertEquals(shiftLeft(Int128.valueOf(0L, 0x0034567890ABCDEFL), 64 + 8), Int128.valueOf(0x34567890ABCDEF00L, 0x0000000000000000L));
        assertEquals(shiftLeft(Int128.valueOf(0L, 0x000000000000CDEFL), 64 + 48), Int128.valueOf(0xCDEF000000000000L, 0x0000000000000000L));
        assertEquals(shiftLeft(Int128.valueOf(0L, 0x1L), 64 + 63), Int128.valueOf(0x8000000000000000L, 0x0000000000000000L));

        assertShiftLeft(new BigInteger("446319580078125"), 19);

        assertShiftLeft(TWO.pow(1), 10);
        assertShiftLeft(TWO.pow(5).add(TWO.pow(1)), 10);
        assertShiftLeft(TWO.pow(1), 100);
        assertShiftLeft(TWO.pow(5).add(TWO.pow(1)), 100);

        assertShiftLeft(TWO.pow(70), 30);
        assertShiftLeft(TWO.pow(70).add(TWO.pow(1)), 30);

        assertShiftLeft(TWO.pow(106), 20);
        assertShiftLeft(TWO.pow(106).add(TWO.pow(1)), 20);
    }

    @Test
    public void testAbsExact()
    {
        assertThat(Int128Math.absExact(Int128.ZERO))
                .isEqualTo(Int128.ZERO);

        assertThat(Int128Math.absExact(Int128.valueOf(1)))
                .isEqualTo(Int128.valueOf(1));

        assertThat(Int128Math.absExact(Int128.valueOf(-1)))
                .isEqualTo(Int128.valueOf(1));

        assertThat(Int128Math.absExact(Int128.MAX_VALUE))
                .isEqualTo(Int128.MAX_VALUE);

        assertThat(Int128Math.absExact(Int128.valueOf(0xC000000000000000L, 0)))
                .isEqualTo(negate(Int128.valueOf(0xC000000000000000L, 0)));

        assertThatThrownBy(() -> Int128Math.absExact(Int128.MIN_VALUE))
                .isInstanceOf(ArithmeticException.class);
    }

    private void assertAdd(Int128 left, Int128 right, Int128 result)
    {
        long[] resultArray = new long[2];
        add(
                left.getHigh(), left.getLow(),
                right.getHigh(), right.getLow(),
                resultArray,
                0);
        assertEquals(Int128.valueOf(resultArray[0], resultArray[1]).toBigInteger(), result.toBigInteger());
    }

    private static void assertInt128ToLongOverflows(BigInteger value)
    {
        Int128 decimal = Int128.valueOf(value);
        assertThatThrownBy(decimal::toLongExact)
                .isInstanceOf(ArithmeticException.class)
                .hasMessage("Overflow");
    }

    private static void assertMultiplyOverflows(Int128 left, Int128 right)
    {
        assertThatThrownBy(() -> multiply(left, right))
                .isInstanceOf(ArithmeticException.class)
                .hasMessage("Overflow");
    }

    private static void assertRescaleOverflows(Int128 decimal, int rescaleFactor)
    {
        assertThatThrownBy(() -> rescale(decimal, rescaleFactor))
                .isInstanceOf(ArithmeticException.class)
                .hasMessage("Overflow");
    }

    private static void assertCompare(Int128 left, Int128 right, int expectedResult)
    {
        assertEquals(left.compareTo(right), expectedResult);
        assertEquals(Int128.compare(left.getHigh(), left.getLow(), right.getHigh(), right.getLow()), expectedResult);
    }

    private static void assertConvertsUnscaledBigIntegerToDecimal(BigInteger value)
    {
        assertEquals(Int128.valueOf(value).toBigInteger(), value);
    }

    private static void assertConvertsUnscaledLongToDecimal(long value)
    {
        assertEquals(Int128.valueOf(value).toLongExact(), value);
        assertEquals(Int128.valueOf(value), Int128.valueOf(BigInteger.valueOf(value)));
    }

    private static void assertShiftRight(Int128 decimal, int rightShifts, boolean roundUp, Int128 expectedResult)
    {
        long[] result = new long[2];
        shiftRight(decimal.getHigh(), decimal.getLow(), rightShifts, roundUp, result, 0);
        assertEquals(Int128.valueOf(result), expectedResult);
    }

    private static void assertDivideAllSigns(int[] dividend, int[] divisor)
    {
        assertDivideAllSigns(valueOf(dividend), 0, valueOf(divisor), 0);
    }

    private void assertShiftLeft(BigInteger value, int leftShifts)
    {
        Int128 decimal = Int128.valueOf(value);
        BigInteger expectedResult = value.multiply(TWO.pow(leftShifts));
        decimal = shiftLeft(decimal, leftShifts);
        assertEquals(decimal.toBigInteger(), expectedResult);
    }

    private static void assertDivideAllSigns(String dividend, String divisor)
    {
        assertDivideAllSigns(dividend, 0, divisor, 0);
    }

    private static void assertDivideAllSigns(String dividend, int dividendRescaleFactor, String divisor, int divisorRescaleFactor)
    {
        assertDivideAllSigns(Int128.valueOf(dividend), dividendRescaleFactor, Int128.valueOf(divisor), divisorRescaleFactor);
    }

    private static void assertDivideAllSigns(Int128 dividend, int dividendRescaleFactor, Int128 divisor, int divisorRescaleFactor)
    {
        assertDivide(dividend, dividendRescaleFactor, divisor, divisorRescaleFactor);
        if (!divisor.isZero()) {
            assertDivide(dividend, dividendRescaleFactor, negate(divisor), divisorRescaleFactor);
        }
        if (!dividend.isZero()) {
            assertDivide(negate(dividend), dividendRescaleFactor, divisor, divisorRescaleFactor);
        }
        if (!dividend.isZero() && !divisor.isZero()) {
            assertDivide(negate(dividend), dividendRescaleFactor, negate(divisor), divisorRescaleFactor);
        }
    }

    private static void assertDivide(Int128 dividend, int dividendRescaleFactor, Int128 divisor, int divisorRescaleFactor)
    {
        BigInteger dividendBigInteger = dividend.toBigInteger();
        BigInteger divisorBigInteger = divisor.toBigInteger();
        BigInteger rescaledDividend = dividendBigInteger.multiply(bigIntegerTenToNth(dividendRescaleFactor));
        BigInteger rescaledDivisor = divisorBigInteger.multiply(bigIntegerTenToNth(divisorRescaleFactor));

        BigInteger expectedQuotient = new BigDecimal(rescaledDividend).divide(new BigDecimal(rescaledDivisor), RoundingMode.HALF_UP).toBigIntegerExact();
        BigInteger expectedRemainder = rescaledDividend.remainder(rescaledDivisor);

        boolean overflowIsExpected = expectedQuotient.abs().compareTo(Int128.MAX_VALUE.toBigInteger()) >= 0 || expectedRemainder.abs().compareTo(Int128.MAX_VALUE.toBigInteger()) >= 0;

        Int128 quotient;
        Int128 remainder;
        try {
            quotient = divideRoundUp(
                    dividend.getHigh(),
                    dividend.getLow(),
                    dividendRescaleFactor,
                    divisor.getHigh(),
                    divisor.getLow(),
                    divisorRescaleFactor);

            remainder = remainder(
                    dividend.getHigh(),
                    dividend.getLow(),
                    dividendRescaleFactor,
                    divisor.getHigh(),
                    divisor.getLow(),
                    divisorRescaleFactor);

            if (overflowIsExpected) {
                fail("overflow is expected");
            }

            BigInteger actualQuotient = quotient.toBigInteger();
            BigInteger actualRemainder = remainder.toBigInteger();

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
        catch (ArithmeticException e) {
            if (!overflowIsExpected) {
                fail("overflow wasn't expected");
            }
        }
    }

    private static boolean isShort(BigInteger value)
    {
        return value.abs().shiftRight(63).equals(BigInteger.ZERO);
    }

    private static void assertMultiply(BigInteger a, long b, BigInteger result)
    {
        assertMultiply(a, BigInteger.valueOf(b), result);
    }

    private static void assertMultiply(BigInteger a, long b, long result)
    {
        assertMultiply(a, BigInteger.valueOf(b), BigInteger.valueOf(result));
    }

    private static void assertMultiply(long a, long b, BigInteger result)
    {
        assertMultiply(BigInteger.valueOf(a), b, result);
    }

    private static void assertMultiply(long a, long b, long result)
    {
        assertMultiply(a, b, BigInteger.valueOf(result));
    }

    private static void assertMultiply(BigInteger a, BigInteger b, BigInteger result)
    {
        assertEquals(Int128.valueOf(result), multiply(Int128.valueOf(a), Int128.valueOf(b)));
        if (isShort(a) && isShort(b)) {
            assertEquals(Int128.valueOf(result), multiply(a.longValue(), b.longValue()));
        }
        if (isShort(a) && !isShort(b)) {
            assertEquals(Int128.valueOf(result), multiply(Int128.valueOf(b), a.longValue()));
        }
        if (!isShort(a) && isShort(b)) {
            assertEquals(Int128.valueOf(result), multiply(Int128.valueOf(a), b.longValue()));
        }
    }

    private static void assertRescale(Int128 decimal, int rescale, Int128 expected)
    {
        assertEquals(rescale(decimal, rescale), expected);

        // test non-zero offset
        long[] result = new long[3];
        rescale(decimal.getHigh(), decimal.getLow(), rescale, result, 1);
        assertEquals(Int128.valueOf(result[1], result[2]), expected);
    }

    private static void assertRescaleTruncate(Int128 decimal, int rescale, Int128 expected)
    {
        assertEquals(rescaleTruncate(decimal, rescale), expected);

        // test non-zero offset
        long[] result = new long[3];
        rescaleTruncate(decimal.getHigh(), decimal.getLow(), rescale, result, 1);
        assertEquals(Int128.valueOf(result[1], result[2]), expected);
    }

    private static Int128 shiftLeft(Int128 value, int shift)
    {
        long[] parts = value.toLongArray();
        Int128Math.shiftLeft(parts, shift);
        return Int128.valueOf(parts);
    }

    private static Int128 valueOf(int[] value)
    {
        checkArgument(value.length == 4, "Expected int[4]");
        return Int128.valueOf(
                (((long) value[0]) << 32) | value[1],
                (((long) value[2]) << 32) | value[3]);
    }
}
