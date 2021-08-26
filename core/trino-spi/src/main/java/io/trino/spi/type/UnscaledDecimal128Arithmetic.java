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

import java.math.BigInteger;
import java.nio.ByteOrder;

import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.trino.spi.type.Decimals.MAX_PRECISION;
import static io.trino.spi.type.Decimals.longTenToNth;
import static java.lang.Integer.toUnsignedLong;
import static java.lang.Math.abs;
import static java.lang.System.arraycopy;
import static java.util.Arrays.fill;

/**
 * 128 bit unscaled decimal arithmetic. The representation is:
 * <p>
 * [127_bit_unscaled_decimal_value sign_bit]
 * 0-bit...........64-bit............128-bit
 * <p>
 * 127_bit_unscaled_decimal_value is stored in little endian format to provide easy conversion to/from long and int types.
 * <p>
 * Based on: https://github.com/apache/hive/blob/master/common/src/java/org/apache/hadoop/hive/common/type/UnsignedInt128.java
 * and https://github.com/apache/hive/blob/master/common/src/java/org/apache/hadoop/hive/common/type/Decimal128.java
 */
public final class UnscaledDecimal128Arithmetic
{
    private static final int NUMBER_OF_LONGS = 2;
    private static final int NUMBER_OF_INTS = 2 * NUMBER_OF_LONGS;

    public static final int UNSCALED_DECIMAL_128_SLICE_LENGTH = NUMBER_OF_LONGS * SIZE_OF_LONG;

    private static final Slice[] POWERS_OF_TEN = new Slice[MAX_PRECISION];
    private static final Slice[] POWERS_OF_FIVE = new Slice[MAX_PRECISION];

    private static final int SIGN_LONG_INDEX = 1;
    private static final int SIGN_INT_INDEX = 3;
    private static final long SIGN_LONG_MASK = 1L << 63;
    private static final int SIGN_INT_MASK = 1 << 31;
    private static final int SIGN_BYTE_MASK = 1 << 7;
    private static final long ALL_BITS_SET_64 = 0xFFFFFFFFFFFFFFFFL;
    private static final long INT_BASE = 1L << 32;

    // Lowest 32 bits of a long
    private static final long LOW_32_BITS = 0xFFFFFFFFL;

    /**
     * 5^13 fits in 2^31.
     */
    private static final int MAX_POWER_OF_FIVE_INT = 13;
    /**
     * 5^x. All unsigned values.
     */
    private static final int[] POWERS_OF_FIVES_INT = new int[MAX_POWER_OF_FIVE_INT + 1];

    /**
     * 5^27 fits in 2^31.
     */
    private static final int MAX_POWER_OF_FIVE_LONG = 27;
    /**
     * 5^x. All unsigned values.
     */
    private static final long[] POWERS_OF_FIVE_LONG = new long[MAX_POWER_OF_FIVE_LONG + 1];
    /**
     * 10^9 fits in 2^31.
     */
    private static final int MAX_POWER_OF_TEN_INT = 9;
    /**
     * 10^18 fits in 2^63.
     */
    private static final int MAX_POWER_OF_TEN_LONG = 18;
    /**
     * 10^x. All unsigned values.
     */
    private static final int[] POWERS_OF_TEN_INT = new int[MAX_POWER_OF_TEN_INT + 1];

    static {
        for (int i = 0; i < POWERS_OF_FIVE.length; ++i) {
            POWERS_OF_FIVE[i] = unscaledDecimal(BigInteger.valueOf(5).pow(i));
        }
        for (int i = 0; i < POWERS_OF_TEN.length; ++i) {
            POWERS_OF_TEN[i] = unscaledDecimal(BigInteger.TEN.pow(i));
        }

        POWERS_OF_FIVES_INT[0] = 1;
        for (int i = 1; i < POWERS_OF_FIVES_INT.length; ++i) {
            POWERS_OF_FIVES_INT[i] = POWERS_OF_FIVES_INT[i - 1] * 5;
        }

        POWERS_OF_FIVE_LONG[0] = 1;
        for (int i = 1; i < POWERS_OF_FIVE_LONG.length; ++i) {
            POWERS_OF_FIVE_LONG[i] = POWERS_OF_FIVE_LONG[i - 1] * 5;
        }

        POWERS_OF_TEN_INT[0] = 1;
        for (int i = 1; i < POWERS_OF_TEN_INT.length; ++i) {
            POWERS_OF_TEN_INT[i] = POWERS_OF_TEN_INT[i - 1] * 10;
        }

        if (!ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN)) {
            throw new IllegalStateException("UnsignedDecimal128Arithmetic is supported on little-endian machines only");
        }
    }

    public static Slice unscaledDecimal()
    {
        return Slices.allocate(UNSCALED_DECIMAL_128_SLICE_LENGTH);
    }

    public static Slice unscaledDecimal(Slice decimal)
    {
        return Slices.copyOf(decimal);
    }

    public static Slice unscaledDecimal(String unscaledValue)
    {
        return unscaledDecimal(new BigInteger(unscaledValue));
    }

    public static Slice unscaledDecimal(BigInteger unscaledValue)
    {
        Slice decimal = Slices.allocate(UNSCALED_DECIMAL_128_SLICE_LENGTH);
        return pack(unscaledValue, decimal);
    }

    public static Slice pack(BigInteger unscaledValue, Slice result)
    {
        pack(0, 0, false, result);
        byte[] bytes = unscaledValue.abs().toByteArray();

        if (bytes.length > UNSCALED_DECIMAL_128_SLICE_LENGTH
                || (bytes.length == UNSCALED_DECIMAL_128_SLICE_LENGTH && (bytes[0] & SIGN_BYTE_MASK) != 0)) {
            throwOverflowException();
        }

        // convert to little-endian order
        reverse(bytes);

        result.setBytes(0, bytes);
        if (unscaledValue.signum() < 0) {
            setNegative(result, true);
        }

        throwIfOverflows(result);

        return result;
    }

    public static Slice unscaledDecimal(long unscaledValue)
    {
        long[] longs = new long[NUMBER_OF_LONGS];
        if (unscaledValue < 0) {
            longs[0] = -unscaledValue;
            longs[1] = SIGN_LONG_MASK;
        }
        else {
            longs[0] = unscaledValue;
        }
        return Slices.wrappedLongArray(longs);
    }

    public static BigInteger unscaledDecimalToBigInteger(Slice decimal)
    {
        byte[] bytes = decimal.getBytes(0, UNSCALED_DECIMAL_128_SLICE_LENGTH);
        // convert to big-endian order
        reverse(bytes);
        bytes[0] &= ~SIGN_BYTE_MASK;
        return new BigInteger(isNegative(decimal) ? -1 : 1, bytes);
    }

    public static long unscaledDecimalToUnscaledLong(Slice decimal)
    {
        long low = getLong(decimal, 0);
        long high = getLong(decimal, 1);
        boolean negative = isNegative(decimal);

        if (high != 0 || ((low > Long.MIN_VALUE || !negative) && low < 0)) {
            throwOverflowException();
        }

        return negative ? -low : low;
    }

    public static long unscaledDecimalToUnscaledLongUnsafe(Slice decimal)
    {
        long low = getLong(decimal, 0);
        return isNegative(decimal) ? -low : low;
    }

    public static Slice rescale(Slice decimal, int rescaleFactor)
    {
        if (rescaleFactor == 0) {
            return decimal;
        }
        else {
            Slice result = unscaledDecimal();
            rescale(decimal, rescaleFactor, result);
            return result;
        }
    }

    public static void rescale(Slice decimal, int rescaleFactor, Slice result)
    {
        if (rescaleFactor == 0) {
            copyUnscaledDecimal(decimal, result);
        }
        else if (rescaleFactor > 0) {
            if (rescaleFactor >= POWERS_OF_TEN.length) {
                throwOverflowException();
            }
            shiftLeftBy10(decimal, rescaleFactor, result);
        }
        else {
            scaleDownRoundUp(decimal, -rescaleFactor, result);
        }
    }

    public static Slice rescale(long decimal, int rescaleFactor)
    {
        Slice result = unscaledDecimal();
        if (rescaleFactor == 0) {
            return unscaledDecimal(decimal);
        }
        else if (rescaleFactor > 0) {
            if (rescaleFactor >= POWERS_OF_TEN.length) {
                throwOverflowException();
            }
            shiftLeftBy10(decimal, rescaleFactor, result);
        }
        else {
            scaleDownRoundUp(unscaledDecimal(decimal), -rescaleFactor, result);
        }

        return result;
    }

    public static Slice rescaleTruncate(Slice decimal, int rescaleFactor)
    {
        if (rescaleFactor == 0) {
            return decimal;
        }
        else {
            Slice result = unscaledDecimal();
            rescaleTruncate(decimal, rescaleFactor, result);
            return result;
        }
    }

    public static void rescaleTruncate(Slice decimal, int rescaleFactor, Slice result)
    {
        if (rescaleFactor == 0) {
            copyUnscaledDecimal(decimal, result);
        }
        else if (rescaleFactor > 0) {
            if (rescaleFactor >= POWERS_OF_TEN.length) {
                throwOverflowException();
            }
            shiftLeftBy10(decimal, rescaleFactor, result);
        }
        else {
            scaleDownTruncate(decimal, -rescaleFactor, result);
        }
    }

    // Multiplies by 10^rescaleFactor. Only positive rescaleFactor values are allowed
    private static void shiftLeftBy10(Slice decimal, int rescaleFactor, Slice result)
    {
        if (rescaleFactor <= MAX_POWER_OF_TEN_INT) {
            multiply(decimal, (int) longTenToNth(rescaleFactor), result);
        }
        else if (rescaleFactor <= MAX_POWER_OF_TEN_LONG) {
            multiply(decimal, longTenToNth(rescaleFactor), result);
        }
        else {
            multiply(POWERS_OF_TEN[rescaleFactor], decimal, result);
        }
    }

    // Multiplies by 10^rescaleFactor. Only positive rescaleFactor values are allowed
    private static void shiftLeftBy10(long decimal, int rescaleFactor, Slice result)
    {
        if (rescaleFactor <= MAX_POWER_OF_TEN_INT) {
            multiply(decimal, (int) longTenToNth(rescaleFactor), result);
        }
        else if (rescaleFactor <= MAX_POWER_OF_TEN_LONG) {
            multiply(decimal, longTenToNth(rescaleFactor), result);
        }
        else {
            multiply(POWERS_OF_TEN[rescaleFactor], decimal, result);
        }
    }

    private static void scaleDownTruncate(Slice decimal, int scaleFactor, Slice result)
    {
        // optimized path for smaller values
        long low = getLong(decimal, 0);
        long high = getLong(decimal, 1);
        if (scaleFactor <= MAX_POWER_OF_TEN_LONG && high == 0 && low >= 0) {
            long divisor = longTenToNth(scaleFactor);
            long newLow = low / divisor;
            pack(result, newLow, 0, isNegative(decimal));
            return;
        }

        // Scales down for 10**rescaleFactor.
        // Because divide by int has limited divisor, we choose code path with the least amount of divisions
        if ((scaleFactor - 1) / MAX_POWER_OF_FIVE_INT < (scaleFactor - 1) / MAX_POWER_OF_TEN_INT) {
            // scale down for 10**rescale is equivalent to scaling down with 5**rescaleFactor first, then with 2**rescaleFactor
            scaleDownFive(decimal, scaleFactor, result);
            shiftRightTruncate(result, scaleFactor, result);
        }
        else {
            scaleDownTenTruncate(decimal, scaleFactor, result);
        }
    }

    public static Slice add(Slice left, Slice right)
    {
        Slice result = unscaledDecimal();
        add(left, right, result);
        return result;
    }

    public static void add(Slice left, Slice right, Slice result)
    {
        long overflow = addWithOverflow(left, right, result);
        if (overflow != 0) {
            throwOverflowException();
        }
    }

    /**
     * Instead of throwing overflow exception, this function returns:
     * 0 when there was no overflow
     * +1 when there was overflow
     * -1 when there was underflow
     */
    public static long addWithOverflow(Slice left, Slice right, Slice result)
    {
        boolean leftNegative = isNegative(left);
        boolean rightNegative = isNegative(right);
        long overflow = 0;
        if (leftNegative == rightNegative) {
            // either both negative or both positive
            overflow = addUnsignedReturnOverflow(left, right, result, leftNegative);
            if (leftNegative) {
                overflow = -overflow;
            }
        }
        else {
            int compare = compareAbsolute(left, right);
            if (compare > 0) {
                subtractUnsigned(left, right, result, leftNegative);
            }
            else if (compare < 0) {
                subtractUnsigned(right, left, result, !leftNegative);
            }
            else {
                setToZero(result);
            }
        }
        return overflow;
    }

    public static Slice subtract(Slice left, Slice right)
    {
        Slice result = unscaledDecimal();
        subtract(left, right, result);
        return result;
    }

    public static void subtract(Slice left, Slice right, Slice result)
    {
        boolean leftNegative = isNegative(left);
        boolean rightNegative = isNegative(right);

        if (leftNegative != rightNegative) {
            // only one is negative
            if (addUnsignedReturnOverflow(left, right, result, leftNegative) != 0) {
                throwOverflowException();
            }
        }
        else {
            int compare = compareAbsolute(left, right);
            if (compare > 0) {
                subtractUnsigned(left, right, result, leftNegative);
            }
            else if (compare < 0) {
                subtractUnsigned(right, left, result, !leftNegative);
            }
            else {
                setToZero(result);
            }
        }
    }

    /**
     * This method ignores signs of the left and right. Returns overflow value.
     */
    private static long addUnsignedReturnOverflow(Slice left, Slice right, Slice result, boolean resultNegative)
    {
        long l0 = getLong(left, 0);
        long l1 = getLong(left, 1);

        long r0 = getLong(right, 0);
        long r1 = getLong(right, 1);

        long z0 = l0 + r0;
        int overflow = unsignedIsSmaller(z0, l0) ? 1 : 0;

        long intermediateResult = l1 + r1 + overflow;
        long z1 = intermediateResult & (~SIGN_LONG_MASK);
        pack(result, z0, z1, resultNegative);

        return intermediateResult >>> 63;
    }

    /**
     * This method ignores signs of the left and right and assumes that left is greater then right
     */
    private static void subtractUnsigned(Slice left, Slice right, Slice result, boolean resultNegative)
    {
        long l0 = getLong(left, 0);
        long l1 = getLong(left, 1);

        long r0 = getLong(right, 0);
        long r1 = getLong(right, 1);

        long z0 = l0 - r0;
        int underflow = unsignedIsSmaller(l0, z0) ? 1 : 0;
        long z1 = l1 - r1 - underflow;

        pack(result, z0, z1, resultNegative);
    }

    public static Slice multiply(Slice left, Slice right)
    {
        Slice result = unscaledDecimal();
        multiply(left, right, result);
        return result;
    }

    public static Slice multiply(Slice left, long right)
    {
        Slice result = unscaledDecimal();
        multiply(left, right, result);
        return result;
    }

    public static Slice multiply(long left, long right)
    {
        Slice result = unscaledDecimal();
        multiply(left, right, result);
        return result;
    }

    public static void multiply(Slice left, Slice right, Slice result)
    {
        checkArgument(result.length() == NUMBER_OF_LONGS * Long.BYTES);

        multiply(getRawLong(left, 0), getRawLong(left, 1), getRawLong(right, 0), getRawLong(right, 1), result);
    }

    public static void multiply(long leftLow, long leftHigh, long rightLow, long rightHigh, Slice result)
    {
        checkArgument(result.length() == NUMBER_OF_LONGS * Long.BYTES);

        long l0 = low(leftLow);
        long l1 = high(leftLow);
        long l2 = low(leftHigh);
        boolean leftNegative = isNegative(leftHigh);
        long l3 = unpackUnsignedInt(high(leftHigh));

        long r0 = low(rightLow);
        long r1 = high(rightLow);
        long r2 = low(rightHigh);
        boolean rightNegative = isNegative(rightHigh);
        long r3 = unpackUnsignedInt(high(rightHigh));

        // the combinations below definitely result in an overflow
        if (((r3 != 0 && (l3 | l2 | l1) != 0) || (r2 != 0 && (l3 | l2) != 0) || (r1 != 0 && l3 != 0))) {
            throwOverflowException();
        }

        long z0 = 0;
        long z1 = 0;
        long z2 = 0;
        long z3 = 0;

        if (l0 != 0) {
            long accumulator = r0 * l0;
            z0 = low(accumulator);
            accumulator = high(accumulator) + r1 * l0;

            z1 = low(accumulator);
            accumulator = high(accumulator) + r2 * l0;

            z2 = low(accumulator);
            accumulator = high(accumulator) + r3 * l0;

            z3 = low(accumulator);

            if (high(accumulator) != 0) {
                throwOverflowException();
            }
        }

        if (l1 != 0) {
            long accumulator = r0 * l1 + z1;
            z1 = low(accumulator);
            accumulator = high(accumulator) + r1 * l1 + z2;

            z2 = low(accumulator);
            accumulator = high(accumulator) + r2 * l1 + z3;

            z3 = low(accumulator);

            if (high(accumulator) != 0) {
                throwOverflowException();
            }
        }

        if (l2 != 0) {
            long accumulator = r0 * l2 + z2;
            z2 = low(accumulator);
            accumulator = high(accumulator) + r1 * l2 + z3;

            z3 = low(accumulator);

            if (high(accumulator) != 0) {
                throwOverflowException();
            }
        }

        if (l3 != 0) {
            long accumulator = r0 * l3 + z3;
            z3 = low(accumulator);

            if (high(accumulator) != 0) {
                throwOverflowException();
            }
        }

        pack(result, (int) z0, (int) z1, (int) z2, (int) z3, leftNegative != rightNegative);
    }

    public static void multiply(Slice left, long right, Slice result)
    {
        checkArgument(result.length() == NUMBER_OF_LONGS * Long.BYTES);

        boolean rightNegative = right < 0;
        if (rightNegative) {
            right = -right;
        }

        multiply(getRawLong(left, 0), getRawLong(left, 1), right, rightNegative ? SIGN_LONG_MASK : 0, result);
    }

    public static void multiply(Slice left, int right, Slice result)
    {
        checkArgument(result.length() == NUMBER_OF_LONGS * Long.BYTES);

        long l0 = toUnsignedLong(getInt(left, 0));
        long l1 = toUnsignedLong(getInt(left, 1));
        long l2 = toUnsignedLong(getInt(left, 2));
        int l3raw = getRawInt(left, 3);
        boolean leftNegative = isNegative(l3raw);
        long l3 = toUnsignedLong(unpackUnsignedInt(l3raw));

        boolean rightNegative = right < 0;
        long r0 = abs(right);

        long z0;
        long z1;
        long z2;
        long z3;

        long accumulator = r0 * l0;
        z0 = low(accumulator);
        accumulator = high(accumulator) + l1 * r0;

        z1 = low(accumulator);
        accumulator = high(accumulator) + l2 * r0;

        z2 = low(accumulator);
        accumulator = high(accumulator) + l3 * r0;

        z3 = low(accumulator);

        if (high(accumulator) != 0) {
            throwOverflowException();
        }

        pack(result, (int) z0, (int) z1, (int) z2, (int) z3, leftNegative != rightNegative);
    }

    // Using multiply(long, long, long, long, Slice) here decreases performance by ~40%
    public static void multiply(long left, long right, Slice result)
    {
        checkArgument(result.length() == NUMBER_OF_LONGS * Long.BYTES);
        boolean rightNegative = right < 0;
        boolean leftNegative = left < 0;
        left = abs(left);
        right = abs(right);

        long l0 = low(left);
        long l1 = high(left);

        long r0 = low(right);
        long r1 = high(right);

        long z0;
        long z1;
        long z2;
        long z3;

        long accumulator = r0 * l0;
        z0 = low(accumulator);
        accumulator = high(accumulator) + r1 * l0;

        z1 = low(accumulator);
        z2 = accumulator >> 32;

        accumulator = r0 * l1 + z1;
        z1 = low(accumulator);
        accumulator = high(accumulator) + r1 * l1 + z2;

        z2 = low(accumulator);
        z3 = high(accumulator);

        pack(result, (int) z0, (int) z1, (int) z2, (int) z3, leftNegative != rightNegative);
    }

    public static void multiply(long left, int right, Slice result)
    {
        checkArgument(result.length() == NUMBER_OF_LONGS * Long.BYTES);
        boolean rightNegative = right < 0;
        boolean leftNegative = left < 0;
        left = abs(left);
        long r0 = abs(right);

        long l0 = low(left);
        long l1 = high(left);

        long z0;
        long z1;
        long z2;

        long accumulator = r0 * l0;
        z0 = low(accumulator);
        z1 = high(accumulator);

        accumulator = r0 * l1 + z1;
        z1 = low(accumulator);
        z2 = high(accumulator);

        pack(result, (int) z0, (int) z1, (int) z2, 0, leftNegative != rightNegative);
    }

    /**
     * This an unsigned operation. Supplying negative arguments will yield wrong results.
     * Assumes left array length to be >= 8. However only first 4 int values are multiplied
     */
    static void multiply256Destructive(int[] left, Slice right)
    {
        long l0 = toUnsignedLong(left[0]);
        long l1 = toUnsignedLong(left[1]);
        long l2 = toUnsignedLong(left[2]);
        long l3 = toUnsignedLong(left[3]);

        long r0 = toUnsignedLong(getInt(right, 0));
        long r1 = toUnsignedLong(getInt(right, 1));
        long r2 = toUnsignedLong(getInt(right, 2));
        long r3 = toUnsignedLong(getInt(right, 3));

        long z0 = 0;
        long z1 = 0;
        long z2 = 0;
        long z3 = 0;
        long z4 = 0;
        long z5 = 0;
        long z6 = 0;
        long z7 = 0;

        if (l0 != 0) {
            long accumulator = r0 * l0;
            z0 = low(accumulator);
            accumulator = high(accumulator) + r1 * l0;

            z1 = low(accumulator);
            accumulator = high(accumulator) + r2 * l0;

            z2 = low(accumulator);
            accumulator = high(accumulator) + r3 * l0;

            z3 = low(accumulator);
            z4 = high(accumulator);
        }

        if (l1 != 0) {
            long accumulator = r0 * l1 + z1;
            z1 = low(accumulator);
            accumulator = high(accumulator) + r1 * l1 + z2;

            z2 = low(accumulator);
            accumulator = high(accumulator) + r2 * l1 + z3;

            z3 = low(accumulator);
            accumulator = high(accumulator) + r3 * l1 + z4;

            z4 = low(accumulator);
            z5 = high(accumulator);
        }

        if (l2 != 0) {
            long accumulator = r0 * l2 + z2;
            z2 = low(accumulator);
            accumulator = high(accumulator) + r1 * l2 + z3;

            z3 = low(accumulator);
            accumulator = high(accumulator) + r2 * l2 + z4;

            z4 = low(accumulator);
            accumulator = high(accumulator) + r3 * l2 + z5;

            z5 = low(accumulator);
            z6 = high(accumulator);
        }

        if (l3 != 0) {
            long accumulator = r0 * l3 + z3;
            z3 = low(accumulator);
            accumulator = high(accumulator) + r1 * l3 + z4;

            z4 = low(accumulator);
            accumulator = high(accumulator) + r2 * l3 + z5;

            z5 = low(accumulator);
            accumulator = high(accumulator) + r3 * l3 + z6;

            z6 = low(accumulator);
            z7 = high(accumulator);
        }

        left[0] = (int) z0;
        left[1] = (int) z1;
        left[2] = (int) z2;
        left[3] = (int) z3;
        left[4] = (int) z4;
        left[5] = (int) z5;
        left[6] = (int) z6;
        left[7] = (int) z7;
    }

    /**
     * This an unsigned operation. Supplying negative arguments will yield wrong results.
     * Assumes left array length to be >= 6. However only first 4 int values are multiplied
     */
    static void multiply256Destructive(int[] left, long right)
    {
        long l0 = toUnsignedLong(left[0]);
        long l1 = toUnsignedLong(left[1]);
        long l2 = toUnsignedLong(left[2]);
        long l3 = toUnsignedLong(left[3]);

        long r0 = low(right);
        long r1 = high(right);

        long z0 = 0;
        long z1 = 0;
        long z2 = 0;
        long z3 = 0;
        long z4 = 0;
        long z5 = 0;

        if (l0 != 0) {
            long accumulator = r0 * l0;
            z0 = low(accumulator);
            accumulator = high(accumulator) + r1 * l0;

            z1 = low(accumulator);
            z2 = high(accumulator);
        }

        if (l1 != 0) {
            long accumulator = r0 * l1 + z1;
            z1 = low(accumulator);
            accumulator = high(accumulator) + r1 * l1 + z2;

            z2 = low(accumulator);
            z3 = high(accumulator);
        }

        if (l2 != 0) {
            long accumulator = r0 * l2 + z2;
            z2 = low(accumulator);
            accumulator = high(accumulator) + r1 * l2 + z3;

            z3 = low(accumulator);
            z4 = high(accumulator);
        }

        if (l3 != 0) {
            long accumulator = r0 * l3 + z3;
            z3 = low(accumulator);
            accumulator = high(accumulator) + r1 * l3 + z4;

            z4 = low(accumulator);
            z5 = high(accumulator);
        }

        left[0] = (int) z0;
        left[1] = (int) z1;
        left[2] = (int) z2;
        left[3] = (int) z3;
        left[4] = (int) z4;
        left[5] = (int) z5;
    }

    /**
     * This an unsigned operation. Supplying negative arguments will yield wrong results.
     * Assumes left array length to be >= 5. However only first 4 int values are multiplied
     */
    static void multiply256Destructive(int[] left, int r0)
    {
        long l0 = toUnsignedLong(left[0]);
        long l1 = toUnsignedLong(left[1]);
        long l2 = toUnsignedLong(left[2]);
        long l3 = toUnsignedLong(left[3]);

        long z0;
        long z1;
        long z2;
        long z3;
        long z4;

        long accumulator = r0 * l0;
        z0 = low(accumulator);
        z1 = high(accumulator);

        accumulator = r0 * l1 + z1;
        z1 = low(accumulator);
        z2 = high(accumulator);

        accumulator = r0 * l2 + z2;
        z2 = low(accumulator);
        z3 = high(accumulator);

        accumulator = r0 * l3 + z3;
        z3 = low(accumulator);
        z4 = high(accumulator);

        left[0] = (int) z0;
        left[1] = (int) z1;
        left[2] = (int) z2;
        left[3] = (int) z3;
        left[4] = (int) z4;
    }

    public static int compare(Slice left, Slice right)
    {
        boolean leftStrictlyNegative = isStrictlyNegative(left);
        boolean rightStrictlyNegative = isStrictlyNegative(right);

        if (leftStrictlyNegative != rightStrictlyNegative) {
            return leftStrictlyNegative ? -1 : 1;
        }
        else {
            return compareAbsolute(left, right) * (leftStrictlyNegative ? -1 : 1);
        }
    }

    public static int compare(long leftRawLow, long leftRawHigh, long rightRawLow, long rightRawHigh)
    {
        boolean leftStrictlyNegative = isStrictlyNegative(leftRawLow, leftRawHigh);
        boolean rightStrictlyNegative = isStrictlyNegative(rightRawLow, rightRawHigh);

        if (leftStrictlyNegative != rightStrictlyNegative) {
            return leftStrictlyNegative ? -1 : 1;
        }
        else {
            long leftHigh = unpackUnsignedLong(leftRawHigh);
            long rightHigh = unpackUnsignedLong(rightRawHigh);
            return compareUnsigned(leftRawLow, leftHigh, rightRawLow, rightHigh) * (leftStrictlyNegative ? -1 : 1);
        }
    }

    public static int compareAbsolute(Slice left, Slice right)
    {
        long leftHigh = getLong(left, 1);
        long rightHigh = getLong(right, 1);
        if (leftHigh != rightHigh) {
            return Long.compareUnsigned(leftHigh, rightHigh);
        }

        long leftLow = getLong(left, 0);
        long rightLow = getLong(right, 0);
        if (leftLow != rightLow) {
            return Long.compareUnsigned(leftLow, rightLow);
        }

        return 0;
    }

    public static int compareUnsigned(long leftRawLow, long leftRawHigh, long rightRawLow, long rightRawHigh)
    {
        if (leftRawHigh != rightRawHigh) {
            return Long.compareUnsigned(leftRawHigh, rightRawHigh);
        }
        if (leftRawLow != rightRawLow) {
            return Long.compareUnsigned(leftRawLow, rightRawLow);
        }
        return 0;
    }

    public static void incrementUnsafe(Slice decimal)
    {
        long low = getLong(decimal, 0);
        if (low != ALL_BITS_SET_64) {
            setRawLong(decimal, 0, low + 1);
            return;
        }

        long high = getLong(decimal, 1);
        setNegativeLong(decimal, high + 1, isNegative(decimal));
    }

    public static void negate(Slice decimal)
    {
        setNegative(decimal, !isNegative(decimal));
    }

    public static boolean isStrictlyNegative(Slice decimal)
    {
        return isStrictlyNegative(getRawLong(decimal, 0), getRawLong(decimal, 1));
    }

    public static boolean isStrictlyNegative(long rawLow, long rawHigh)
    {
        return isNegative(rawHigh) && (rawLow != 0 || unpackUnsignedLong(rawHigh) != 0);
    }

    private static boolean isNegative(int lastRawHigh)
    {
        return lastRawHigh >>> 31 != 0;
    }

    public static boolean isNegative(Slice decimal)
    {
        return isNegative(getRawInt(decimal, SIGN_INT_INDEX));
    }

    public static boolean isNegative(long rawHigh)
    {
        return rawHigh >>> 63 != 0;
    }

    public static boolean isZero(Slice decimal)
    {
        return getLong(decimal, 0) == 0 && getLong(decimal, 1) == 0;
    }

    public static String toUnscaledString(Slice decimal)
    {
        if (isZero(decimal)) {
            return "0";
        }

        char[] buffer = new char[MAX_PRECISION + 1];
        int index = buffer.length;
        boolean negative = isNegative(decimal);
        decimal = unscaledDecimal(decimal);
        do {
            int remainder = divide(decimal, 10, decimal);
            buffer[--index] = (char) ('0' + remainder);
        }
        while (!isZero(decimal));

        if (negative) {
            buffer[--index] = '-';
        }

        return new String(buffer, index, buffer.length - index);
    }

    public static boolean overflows(Slice value, int precision)
    {
        if (precision == MAX_PRECISION) {
            return exceedsOrEqualTenToThirtyEight(value);
        }
        return precision < MAX_PRECISION && compareAbsolute(value, POWERS_OF_TEN[precision]) >= 0;
    }

    public static void throwIfOverflows(Slice decimal)
    {
        if (exceedsOrEqualTenToThirtyEight(decimal)) {
            throwOverflowException();
        }
    }

    public static void throwIfOverflows(Slice value, int precision)
    {
        if (overflows(value, precision)) {
            throwOverflowException();
        }
    }

    private static void scaleDownRoundUp(Slice decimal, int scaleFactor, Slice result)
    {
        // optimized path for smaller values
        long low = getLong(decimal, 0);
        long high = getLong(decimal, 1);
        if (scaleFactor <= MAX_POWER_OF_TEN_LONG && high == 0 && low >= 0) {
            long divisor = longTenToNth(scaleFactor);
            long newLow = low / divisor;
            if (low % divisor >= (divisor >> 1)) {
                newLow++;
            }
            pack(result, newLow, 0, isNegative(decimal));
            return;
        }

        // Scales down for 10**rescaleFactor.
        // Because divide by int has limited divisor, we choose code path with the least amount of divisions
        if ((scaleFactor - 1) / MAX_POWER_OF_FIVE_INT < (scaleFactor - 1) / MAX_POWER_OF_TEN_INT) {
            // scale down for 10**rescale is equivalent to scaling down with 5**rescaleFactor first, then with 2**rescaleFactor
            scaleDownFive(decimal, scaleFactor, result);
            shiftRightRoundUp(result, scaleFactor, result);
        }
        else {
            scaleDownTenRoundUp(decimal, scaleFactor, result);
        }
    }

    /**
     * Scale down the value for 5**fiveScale (result := decimal / 5**fiveScale).
     */
    private static void scaleDownFive(Slice decimal, int fiveScale, Slice result)
    {
        while (true) {
            int powerFive = Math.min(fiveScale, MAX_POWER_OF_FIVE_INT);
            fiveScale -= powerFive;

            int divisor = POWERS_OF_FIVES_INT[powerFive];
            divide(decimal, divisor, result);
            decimal = result;

            if (fiveScale == 0) {
                return;
            }
        }
    }

    /**
     * Scale down the value for 10**tenScale (this := this / 5**tenScale). This
     * method rounds-up, eg 44/10=4, 44/10=5.
     */
    private static void scaleDownTenRoundUp(Slice decimal, int tenScale, Slice result)
    {
        boolean round;
        do {
            int powerTen = Math.min(tenScale, MAX_POWER_OF_TEN_INT);
            tenScale -= powerTen;

            int divisor = POWERS_OF_TEN_INT[powerTen];
            round = divideCheckRound(decimal, divisor, result);
            decimal = result;
        }
        while (tenScale > 0);

        if (round) {
            incrementUnsafe(decimal);
        }
    }

    private static void scaleDownTenTruncate(Slice decimal, int tenScale, Slice result)
    {
        do {
            int powerTen = Math.min(tenScale, MAX_POWER_OF_TEN_INT);
            tenScale -= powerTen;

            int divisor = POWERS_OF_TEN_INT[powerTen];
            divide(decimal, divisor, result);
            decimal = result;
        }
        while (tenScale > 0);
    }

    private static void shiftRightRoundUp(Slice decimal, int rightShifts, Slice result)
    {
        shiftRight(decimal, rightShifts, true, result);
    }

    private static void shiftRightTruncate(Slice decimal, int rightShifts, Slice result)
    {
        shiftRight(decimal, rightShifts, false, result);
    }

    // visible for testing
    static void shiftRight(Slice decimal, int rightShifts, boolean roundUp, Slice result)
    {
        if (rightShifts == 0) {
            copyUnscaledDecimal(decimal, result);
            return;
        }

        int wordShifts = rightShifts / 64;
        int bitShiftsInWord = rightShifts % 64;
        int shiftRestore = 64 - bitShiftsInWord;

        // check round-ups before settings values to result.
        // be aware that result could be the same object as decimal.
        boolean roundCarry;
        if (bitShiftsInWord == 0) {
            roundCarry = roundUp && getLong(decimal, wordShifts - 1) < 0;
        }
        else {
            roundCarry = roundUp && (getLong(decimal, wordShifts) & (1L << (bitShiftsInWord - 1))) != 0;
        }

        // Store negative before settings values to result.
        boolean negative = isNegative(decimal);

        long low;
        long high;

        switch (wordShifts) {
            case 0:
                low = getLong(decimal, 0);
                high = getLong(decimal, 1);
                break;
            case 1:
                low = getLong(decimal, 1);
                high = 0;
                break;
            default:
                throw new IllegalArgumentException();
        }

        if (bitShiftsInWord > 0) {
            low = (low >>> bitShiftsInWord) | (high << shiftRestore);
            high = (high >>> bitShiftsInWord);
        }

        if (roundCarry) {
            if (low != ALL_BITS_SET_64) {
                low++;
            }
            else {
                low = 0;
                high++;
            }
        }

        pack(result, low, high, negative);
    }

    public static Slice divideRoundUp(long dividend, int dividendScaleFactor, long divisor)
    {
        return divideRoundUp(
                abs(dividend), dividend < 0 ? SIGN_LONG_MASK : 0, dividendScaleFactor,
                abs(divisor), divisor < 0 ? SIGN_LONG_MASK : 0);
    }

    public static Slice divideRoundUp(long dividend, int dividendScaleFactor, Slice divisor)
    {
        return divideRoundUp(
                abs(dividend), dividend < 0 ? SIGN_LONG_MASK : 0, dividendScaleFactor,
                getRawLong(divisor, 0), getRawLong(divisor, 1));
    }

    public static Slice divideRoundUp(Slice dividend, int dividendScaleFactor, long divisor)
    {
        return divideRoundUp(
                getRawLong(dividend, 0), getRawLong(dividend, 1), dividendScaleFactor,
                abs(divisor), divisor < 0 ? SIGN_LONG_MASK : 0);
    }

    public static Slice divideRoundUp(Slice dividend, int dividendScaleFactor, Slice divisor)
    {
        return divideRoundUp(
                getRawLong(dividend, 0), getRawLong(dividend, 1), dividendScaleFactor,
                getRawLong(divisor, 0), getRawLong(divisor, 1));
    }

    private static Slice divideRoundUp(long dividendLow, long dividendHigh, int dividendScaleFactor, long divisorLow, long divisorHigh)
    {
        Slice quotient = unscaledDecimal();
        Slice remainder = unscaledDecimal();
        divide(dividendLow, dividendHigh, dividendScaleFactor, divisorLow, divisorHigh, 0, quotient, remainder);

        // round
        boolean quotientIsNegative = isNegative(quotient);
        setNegative(quotient, false);
        setNegative(remainder, false);

        // if (2 * remainder >= divisor) - increment quotient by one
        shiftLeftDestructive(remainder, 1);
        long remainderLow = getRawLong(remainder, 0);
        long remainderHigh = getRawLong(remainder, 1);
        long divisorHighUnsigned = unpackUnsignedLong(divisorHigh);
        if (compareUnsigned(remainderLow, remainderHigh, divisorLow, divisorHighUnsigned) >= 0) {
            incrementUnsafe(quotient);
            throwIfOverflows(quotient);
        }

        setNegative(quotient, quotientIsNegative);
        return quotient;
    }

    // visible for testing
    static Slice shiftLeft(Slice decimal, int leftShifts)
    {
        Slice result = Slices.copyOf(decimal);
        shiftLeftDestructive(result, leftShifts);
        return result;
    }

    // visible for testing
    static void shiftLeftDestructive(Slice decimal, int leftShifts)
    {
        if (leftShifts == 0) {
            return;
        }

        int wordShifts = leftShifts / 64;
        int bitShiftsInWord = leftShifts % 64;
        int shiftRestore = 64 - bitShiftsInWord;

        // check overflow
        if (bitShiftsInWord != 0) {
            if ((getLong(decimal, 1 - wordShifts) & (-1L << shiftRestore)) != 0) {
                throwOverflowException();
            }
        }
        if (wordShifts == 1) {
            if (getLong(decimal, 1) != 0) {
                throwOverflowException();
            }
        }

        // Store negative before settings values to result.
        boolean negative = isNegative(decimal);

        long low;
        long high;

        switch (wordShifts) {
            case 0:
                low = getLong(decimal, 0);
                high = getLong(decimal, 1);
                break;
            case 1:
                low = 0;
                high = getLong(decimal, 0);
                break;
            default:
                throw new IllegalArgumentException();
        }

        if (bitShiftsInWord > 0) {
            high = (high << bitShiftsInWord) | (low >>> shiftRestore);
            low = (low << bitShiftsInWord);
        }

        pack(decimal, low, high, negative);
    }

    public static Slice remainder(long dividend, int dividendScaleFactor, long divisor, int divisorScaleFactor)
    {
        return remainder(
                abs(dividend), dividend < 0 ? SIGN_LONG_MASK : 0, dividendScaleFactor,
                abs(divisor), divisor < 0 ? SIGN_LONG_MASK : 0, divisorScaleFactor);
    }

    public static Slice remainder(long dividend, int dividendScaleFactor, Slice divisor, int divisorScaleFactor)
    {
        return remainder(
                abs(dividend), dividend < 0 ? SIGN_LONG_MASK : 0, dividendScaleFactor,
                getRawLong(divisor, 0), getRawLong(divisor, 1), divisorScaleFactor);
    }

    public static Slice remainder(Slice dividend, int dividendScaleFactor, long divisor, int divisorScaleFactor)
    {
        return remainder(
                getRawLong(dividend, 0), getRawLong(dividend, 1), dividendScaleFactor,
                abs(divisor), divisor < 0 ? SIGN_LONG_MASK : 0, divisorScaleFactor);
    }

    public static Slice remainder(Slice dividend, int dividendScaleFactor, Slice divisor, int divisorScaleFactor)
    {
        return remainder(
                getRawLong(dividend, 0), getRawLong(dividend, 1), dividendScaleFactor,
                getRawLong(divisor, 0), getRawLong(divisor, 1), divisorScaleFactor);
    }

    private static Slice remainder(long dividendLow, long dividendHigh, int dividendScaleFactor, long divisorLow, long divisorHigh, int divisorScaleFactor)
    {
        Slice quotient = unscaledDecimal();
        Slice remainder = unscaledDecimal();
        divide(dividendLow, dividendHigh, dividendScaleFactor, divisorLow, divisorHigh, divisorScaleFactor, quotient, remainder);
        return remainder;
    }

    // visible for testing
    static void divide(Slice dividend, int dividendScaleFactor, Slice divisor, int divisorScaleFactor, Slice quotient, Slice remainder)
    {
        divide(getRawLong(dividend, 0), getRawLong(dividend, 1), dividendScaleFactor, getRawLong(divisor, 0), getRawLong(divisor, 1), divisorScaleFactor, quotient, remainder);
    }

    private static void divide(long dividendLow, long dividendHigh, int dividendScaleFactor, long divisorLow, long divisorHigh, int divisorScaleFactor, Slice quotient, Slice remainder)
    {
        if (compare(divisorLow, divisorHigh, 0, 0) == 0) {
            throwDivisionByZeroException();
        }

        if (dividendScaleFactor >= MAX_PRECISION) {
            throwOverflowException();
        }

        if (divisorScaleFactor >= MAX_PRECISION) {
            throwOverflowException();
        }

        boolean dividendIsNegative = isNegative(dividendHigh);
        boolean divisorIsNegative = isNegative(divisorHigh);
        boolean quotientIsNegative = (dividendIsNegative != divisorIsNegative);

        // to fit 128b * 128b * 32b unsigned multiplication
        int[] dividend = new int[NUMBER_OF_INTS * 2 + 1];
        dividend[0] = lowInt(dividendLow);
        dividend[1] = highInt(dividendLow);
        dividend[2] = lowInt(dividendHigh);
        dividend[3] = (highInt(dividendHigh) & ~SIGN_INT_MASK);

        if (dividendScaleFactor > 0) {
            shiftLeftBy5Destructive(dividend, dividendScaleFactor);
            shiftLeftMultiPrecision(dividend, NUMBER_OF_INTS * 2, dividendScaleFactor);
        }

        int[] divisor = new int[NUMBER_OF_INTS * 2];
        divisor[0] = lowInt(divisorLow);
        divisor[1] = highInt(divisorLow);
        divisor[2] = lowInt(divisorHigh);
        divisor[3] = (highInt(divisorHigh) & ~SIGN_INT_MASK);

        if (divisorScaleFactor > 0) {
            shiftLeftBy5Destructive(divisor, divisorScaleFactor);
            shiftLeftMultiPrecision(divisor, NUMBER_OF_INTS * 2, divisorScaleFactor);
        }

        int[] multiPrecisionQuotient = new int[NUMBER_OF_INTS * 2];
        divideUnsignedMultiPrecision(dividend, divisor, multiPrecisionQuotient);

        pack(multiPrecisionQuotient, quotient, quotientIsNegative);
        pack(dividend, remainder, dividendIsNegative);
        throwIfOverflows(quotient);
        throwIfOverflows(remainder);
    }

    /**
     * Value must have a length of 8
     */
    private static void shiftLeftBy5Destructive(int[] value, int shift)
    {
        if (shift <= MAX_POWER_OF_FIVE_INT) {
            multiply256Destructive(value, POWERS_OF_FIVES_INT[shift]);
        }
        else if (shift < MAX_POWER_OF_TEN_LONG) {
            multiply256Destructive(value, POWERS_OF_FIVE_LONG[shift]);
        }
        else {
            multiply256Destructive(value, POWERS_OF_FIVE[shift]);
        }
    }

    /**
     * Divides mutableDividend / mutable divisor
     * Places quotient in first argument and reminder in first argument
     */
    private static void divideUnsignedMultiPrecision(int[] dividend, int[] divisor, int[] quotient)
    {
        checkArgument(dividend.length == NUMBER_OF_INTS * 2 + 1);
        checkArgument(divisor.length == NUMBER_OF_INTS * 2);
        checkArgument(quotient.length == NUMBER_OF_INTS * 2);

        int divisorLength = digitsInIntegerBase(divisor);
        int dividendLength = digitsInIntegerBase(dividend);

        if (dividendLength < divisorLength) {
            return;
        }

        if (divisorLength == 1) {
            int remainder = divideUnsignedMultiPrecision(dividend, dividendLength, divisor[0]);
            checkState(dividend[dividend.length - 1] == 0);
            arraycopy(dividend, 0, quotient, 0, quotient.length);
            fill(dividend, 0);
            dividend[0] = remainder;
            return;
        }

        // normalize divisor. Most significant divisor word must be > BASE/2
        // effectively it can be achieved by shifting divisor left until the leftmost bit is 1
        int nlz = Integer.numberOfLeadingZeros(divisor[divisorLength - 1]);
        shiftLeftMultiPrecision(divisor, divisorLength, nlz);
        int normalizedDividendLength = Math.min(dividend.length, dividendLength + 1);
        shiftLeftMultiPrecision(dividend, normalizedDividendLength, nlz);

        divideKnuthNormalized(dividend, normalizedDividendLength, divisor, divisorLength, quotient);

        // un-normalize remainder which is stored in dividend
        shiftRightMultiPrecision(dividend, normalizedDividendLength, nlz);
    }

    private static void divideKnuthNormalized(int[] remainder, int dividendLength, int[] divisor, int divisorLength, int[] quotient)
    {
        int v1 = divisor[divisorLength - 1];
        int v0 = divisor[divisorLength - 2];
        for (int reminderIndex = dividendLength - 1; reminderIndex >= divisorLength; reminderIndex--) {
            int qHat = estimateQuotient(remainder[reminderIndex], remainder[reminderIndex - 1], remainder[reminderIndex - 2], v1, v0);
            if (qHat != 0) {
                boolean overflow = multiplyAndSubtractUnsignedMultiPrecision(remainder, reminderIndex, divisor, divisorLength, qHat);
                // Add back - probability is 2**(-31). R += D. Q[digit] -= 1
                if (overflow) {
                    qHat--;
                    addUnsignedMultiPrecision(remainder, reminderIndex, divisor, divisorLength);
                }
            }
            quotient[reminderIndex - divisorLength] = qHat;
        }
    }

    /**
     * Use the Knuth notation
     * <p>
     * u{x} - dividend
     * v{v} - divisor
     */
    private static int estimateQuotient(int u2, int u1, int u0, int v1, int v0)
    {
        // estimate qhat based on the first 2 digits of divisor divided by the first digit of a dividend
        long u21 = combineInts(u2, u1);
        long qhat;
        if (u2 == v1) {
            qhat = INT_BASE - 1;
        }
        else if (u21 >= 0) {
            qhat = u21 / toUnsignedLong(v1);
        }
        else {
            qhat = divideUnsignedLong(u21, v1);
        }

        if (qhat == 0) {
            return 0;
        }

        // Check if qhat is greater than expected considering only first 3 digits of a dividend
        // This step help to eliminate all the cases when the estimation is greater than q by 2
        // and eliminates most of the cases when qhat is greater than q by 1
        //
        // u2 * b * b + u1 * b + u0 >= (v1 * b + v0) * qhat
        // u2 * b * b + u1 * b + u0 >= v1 * b * qhat + v0 * qhat
        // u2 * b * b + u1 * b - v1 * b * qhat >=  v0 * qhat - u0
        // (u21 - v1 * qhat) * b >=  v0 * qhat - u0
        // (u21 - v1 * qhat) * b + u0 >=  v0 * qhat
        // When ((u21 - v1 * qhat) * b + u0) is less than (v0 * qhat) decrease qhat by one

        int iterations = 0;
        long rhat = u21 - toUnsignedLong(v1) * qhat;
        while (Long.compareUnsigned(rhat, INT_BASE) < 0 && Long.compareUnsigned(toUnsignedLong(v0) * qhat, combineInts(lowInt(rhat), u0)) > 0) {
            iterations++;
            qhat--;
            rhat += toUnsignedLong(v1);
        }

        if (iterations > 2) {
            throw new IllegalStateException("qhat is greater than q by more than 2: " + iterations);
        }

        return (int) qhat;
    }

    private static long divideUnsignedLong(long dividend, int divisor)
    {
        long unsignedDivisor = toUnsignedLong(divisor);

        if (dividend > 0) {
            return dividend / unsignedDivisor;
        }

        // HD 9-3, 4) q = divideUnsigned(n, 2) / d * 2
        long quotient = ((dividend >>> 1) / unsignedDivisor) * 2;
        long remainder = dividend - quotient * unsignedDivisor;

        if (Long.compareUnsigned(remainder, unsignedDivisor) >= 0) {
            quotient++;
        }

        return quotient;
    }

    /**
     * Calculate multi-precision [left - right * multiplier] with given left offset and length.
     * Return true when overflow occurred
     */
    private static boolean multiplyAndSubtractUnsignedMultiPrecision(int[] left, int leftOffset, int[] right, int length, int multiplier)
    {
        long unsignedMultiplier = toUnsignedLong(multiplier);
        int leftIndex = leftOffset - length;
        long multiplyAccumulator = 0;
        long subtractAccumulator = INT_BASE;
        for (int rightIndex = 0; rightIndex < length; rightIndex++, leftIndex++) {
            multiplyAccumulator = toUnsignedLong(right[rightIndex]) * unsignedMultiplier + multiplyAccumulator;
            subtractAccumulator = (subtractAccumulator + toUnsignedLong(left[leftIndex])) - toUnsignedLong(lowInt(multiplyAccumulator));
            multiplyAccumulator = high(multiplyAccumulator);
            left[leftIndex] = lowInt(subtractAccumulator);
            subtractAccumulator = high(subtractAccumulator) + INT_BASE - 1;
        }
        subtractAccumulator += toUnsignedLong(left[leftIndex]) - multiplyAccumulator;
        left[leftIndex] = lowInt(subtractAccumulator);
        return highInt(subtractAccumulator) == 0;
    }

    private static void addUnsignedMultiPrecision(int[] left, int leftOffset, int[] right, int length)
    {
        int leftIndex = leftOffset - length;
        int carry = 0;
        for (int rightIndex = 0; rightIndex < length; rightIndex++, leftIndex++) {
            long accumulator = toUnsignedLong(left[leftIndex]) + toUnsignedLong(right[rightIndex]) + toUnsignedLong(carry);
            left[leftIndex] = lowInt(accumulator);
            carry = highInt(accumulator);
        }
        left[leftIndex] += carry;
    }

    // visible for testing
    static int[] shiftLeftMultiPrecision(int[] number, int length, int shifts)
    {
        if (shifts == 0) {
            return number;
        }
        // wordShifts = shifts / 32
        int wordShifts = shifts >>> 5;
        // we don't wan't to loose any leading bits
        for (int i = 0; i < wordShifts; i++) {
            checkState(number[length - i - 1] == 0);
        }
        if (wordShifts > 0) {
            arraycopy(number, 0, number, wordShifts, length - wordShifts);
            fill(number, 0, wordShifts, 0);
        }
        // bitShifts = shifts % 32
        int bitShifts = shifts & 0b11111;
        if (bitShifts > 0) {
            // we don't wan't to loose any leading bits
            checkState(number[length - 1] >>> (Integer.SIZE - bitShifts) == 0);
            for (int position = length - 1; position > 0; position--) {
                number[position] = (number[position] << bitShifts) | (number[position - 1] >>> (Integer.SIZE - bitShifts));
            }
            number[0] = number[0] << bitShifts;
        }
        return number;
    }

    // visible for testing
    static int[] shiftRightMultiPrecision(int[] number, int length, int shifts)
    {
        if (shifts == 0) {
            return number;
        }
        // wordShifts = shifts / 32
        int wordShifts = shifts >>> 5;
        // we don't wan't to loose any trailing bits
        for (int i = 0; i < wordShifts; i++) {
            checkState(number[i] == 0);
        }
        if (wordShifts > 0) {
            arraycopy(number, wordShifts, number, 0, length - wordShifts);
            fill(number, length - wordShifts, length, 0);
        }
        // bitShifts = shifts % 32
        int bitShifts = shifts & 0b11111;
        if (bitShifts > 0) {
            // we don't wan't to loose any trailing bits
            checkState(number[0] << (Integer.SIZE - bitShifts) == 0);
            for (int position = 0; position < length - 1; position++) {
                number[position] = (number[position] >>> bitShifts) | (number[position + 1] << (Integer.SIZE - bitShifts));
            }
            number[length - 1] = number[length - 1] >>> bitShifts;
        }
        return number;
    }

    private static int divideUnsignedMultiPrecision(int[] dividend, int dividendLength, int divisor)
    {
        if (divisor == 0) {
            throwDivisionByZeroException();
        }

        if (dividendLength == 1) {
            long dividendUnsigned = toUnsignedLong(dividend[0]);
            long divisorUnsigned = toUnsignedLong(divisor);
            long quotient = dividendUnsigned / divisorUnsigned;
            long remainder = dividendUnsigned - (divisorUnsigned * quotient);
            dividend[0] = (int) quotient;
            return (int) remainder;
        }

        long divisorUnsigned = toUnsignedLong(divisor);
        long remainder = 0;
        for (int dividendIndex = dividendLength - 1; dividendIndex >= 0; dividendIndex--) {
            remainder = (remainder << 32) + toUnsignedLong(dividend[dividendIndex]);
            long quotient = divideUnsignedLong(remainder, divisor);
            dividend[dividendIndex] = (int) quotient;
            remainder = remainder - (quotient * divisorUnsigned);
        }
        return (int) remainder;
    }

    private static int digitsInIntegerBase(int[] digits)
    {
        int length = digits.length;
        while (length > 0 && digits[length - 1] == 0) {
            length--;
        }
        return length;
    }

    private static long combineInts(int high, int low)
    {
        return (((long) high) << 32) | toUnsignedLong(low);
    }

    private static long high(long value)
    {
        return value >>> 32;
    }

    private static long low(long value)
    {
        return value & LOW_32_BITS;
    }

    private static int highInt(long val)
    {
        return (int) (high(val));
    }

    private static int lowInt(long val)
    {
        return (int) val;
    }

    private static void pack(int[] digits, Slice decimal, boolean negative)
    {
        if (digitsInIntegerBase(digits) > NUMBER_OF_INTS) {
            throwOverflowException();
        }
        if ((digits[3] & SIGN_INT_MASK) != 0) {
            throwOverflowException();
        }
        pack(decimal, digits[0], digits[1], digits[2], digits[3], negative);
    }

    private static boolean divideCheckRound(Slice decimal, int divisor, Slice result)
    {
        int remainder = divide(decimal, divisor, result);
        return (remainder >= (divisor >> 1));
    }

    private static int divide(Slice decimal, int divisor, Slice result)
    {
        if (divisor == 0) {
            throwDivisionByZeroException();
        }
        checkArgument(divisor > 0);

        long remainder = getLong(decimal, 1);
        long high = remainder / divisor;
        remainder %= divisor;

        remainder = toUnsignedLong(getInt(decimal, 1)) + (remainder << 32);
        int z1 = (int) (remainder / divisor);
        remainder %= divisor;

        remainder = toUnsignedLong(getInt(decimal, 0)) + (remainder << 32);
        int z0 = (int) (remainder / divisor);

        pack(result, z0, z1, high, isNegative(decimal));
        return (int) (remainder % divisor);
    }

    private static void throwDivisionByZeroException()
    {
        throw new ArithmeticException("Division by zero");
    }

    private static void setNegative(Slice decimal, boolean negative)
    {
        setRawInt(decimal, SIGN_INT_INDEX, getInt(decimal, SIGN_INT_INDEX) | (negative ? SIGN_INT_MASK : 0));
    }

    private static void setNegativeInt(Slice decimal, int v3, boolean negative)
    {
        if (v3 < 0) {
            throwOverflowException();
        }
        setRawInt(decimal, SIGN_INT_INDEX, v3 | (negative ? SIGN_INT_MASK : 0));
    }

    private static void setNegativeLong(Slice decimal, long high, boolean negative)
    {
        setRawLong(decimal, SIGN_LONG_INDEX, high | (negative ? SIGN_LONG_MASK : 0));
    }

    private static void copyUnscaledDecimal(Slice from, Slice to)
    {
        setRawLong(to, 0, getRawLong(from, 0));
        setRawLong(to, 1, getRawLong(from, 1));
    }

    private static void pack(Slice decimal, int v0, int v1, int v2, int v3, boolean negative)
    {
        // check if the result is zero and if so, don't create negative zeros:
        negative &= v0 != 0 | v1 != 0 | v2 != 0 | v3 != 0;
        setRawInt(decimal, 0, v0);
        setRawInt(decimal, 1, v1);
        setRawInt(decimal, 2, v2);
        setNegativeInt(decimal, v3, negative);
    }

    private static void pack(Slice decimal, int v0, int v1, long high, boolean negative)
    {
        // check if the result is zero and if so, don't create negative zeros:
        negative &= v0 != 0 | v1 != 0 | high != 0;
        setRawInt(decimal, 0, v0);
        setRawInt(decimal, 1, v1);
        setNegativeLong(decimal, high, negative);
    }

    private static void pack(Slice decimal, long low, long high, boolean negative)
    {
        // check if the result is zero and if so, don't create negative zeros:
        negative &= low != 0 | high != 0;
        setRawLong(decimal, 0, low);
        setNegativeLong(decimal, high, negative);
    }

    public static Slice pack(long low, long high, boolean negative)
    {
        Slice decimal = unscaledDecimal();
        pack(low, high, negative, decimal);
        return decimal;
    }

    public static void pack(long low, long high, boolean negative, Slice result)
    {
        setRawLong(result, 0, low);
        setRawLong(result, 1, high | (negative ? SIGN_LONG_MASK : 0));
    }

    public static void pack(long low, long high, boolean negative, Slice result, int resultOffset)
    {
        result.setLong(resultOffset, low);

        long value = high | (negative ? SIGN_LONG_MASK : 0);
        result.setLong(resultOffset + SIZE_OF_LONG, value);
    }

    public static void pack(long low, long high, boolean negative, long[] result, int resultOffset)
    {
        result[resultOffset] = low;
        long value = high | (negative ? SIGN_LONG_MASK : 0);
        result[resultOffset + 1] = value;
    }

    public static void throwOverflowException()
    {
        throw new ArithmeticException("Decimal overflow");
    }

    public static int getInt(Slice decimal, int index)
    {
        int value = getRawInt(decimal, index);
        if (index == SIGN_INT_INDEX) {
            value &= ~SIGN_INT_MASK;
        }
        return value;
    }

    public static long getLong(Slice decimal, int index)
    {
        long value = getRawLong(decimal, index);
        if (index == SIGN_LONG_INDEX) {
            return unpackUnsignedLong(value);
        }
        return value;
    }

    private static boolean exceedsOrEqualTenToThirtyEight(Slice decimal)
    {
        // 10**38=
        // i0 = 0(0), i1 = 160047680(98a22400), i2 = 1518781562(5a86c47a), i3 = 1262177448(4b3b4ca8)
        // low = 0x98a2240000000000l, high = 0x4b3b4ca85a86c47al
        long high = getLong(decimal, 1);
        if (high >= 0 && high < 0x4b3b4ca85a86c47aL) {
            return false;
        }
        if (high != 0x4b3b4ca85a86c47aL) {
            return true;
        }

        long low = getLong(decimal, 0);
        return low < 0 || low >= 0x098a224000000000L;
    }

    private static void reverse(final byte[] a)
    {
        final int length = a.length;
        for (int i = length / 2; i-- != 0; ) {
            final byte t = a[length - i - 1];
            a[length - i - 1] = a[i];
            a[i] = t;
        }
    }

    private static void setToZero(Slice decimal)
    {
        for (int i = 0; i < NUMBER_OF_LONGS; i++) {
            setRawLong(decimal, i, 0);
        }
    }

    private static int unpackUnsignedInt(int value)
    {
        return value & ~SIGN_INT_MASK;
    }

    private static long unpackUnsignedInt(long value)
    {
        return value & ~SIGN_INT_MASK;
    }

    private static long unpackUnsignedLong(long value)
    {
        return value & ~SIGN_LONG_MASK;
    }

    private static int getRawInt(Slice decimal, int index)
    {
        return decimal.getInt(SIZE_OF_INT * index);
    }

    private static void setRawInt(Slice decimal, int index, int value)
    {
        decimal.setInt(SIZE_OF_INT * index, value);
    }

    private static long getRawLong(Slice decimal, int index)
    {
        return decimal.getLong(SIZE_OF_LONG * index);
    }

    private static void setRawLong(Slice decimal, int index, long value)
    {
        decimal.setLong(SIZE_OF_LONG * index, value);
    }

    /**
     * Based on Long.compareUnsigned()
     */
    private static boolean unsignedIsSmaller(long first, long second)
    {
        return first + Long.MIN_VALUE < second + Long.MIN_VALUE;
    }

    private static void checkArgument(boolean condition)
    {
        if (!condition) {
            throw new IllegalArgumentException();
        }
    }

    private static void checkState(boolean condition)
    {
        if (!condition) {
            throw new IllegalStateException();
        }
    }

    private UnscaledDecimal128Arithmetic() {}
}
