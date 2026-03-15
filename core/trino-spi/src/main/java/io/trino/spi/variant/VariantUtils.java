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
package io.trino.spi.variant;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.XxHash64;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Int128Math;

import java.lang.runtime.ExactConversionsSupport;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Supplier;

import static io.trino.spi.type.Decimals.longTenToNth;
import static io.trino.spi.variant.Header.BasicType.SHORT_STRING;
import static io.trino.spi.variant.Header.PrimitiveType;
import static io.trino.spi.variant.Header.PrimitiveType.DOUBLE;
import static io.trino.spi.variant.Header.PrimitiveType.FLOAT;
import static io.trino.spi.variant.Header.PrimitiveType.INT16;
import static io.trino.spi.variant.Header.PrimitiveType.INT32;
import static io.trino.spi.variant.Header.PrimitiveType.INT64;
import static io.trino.spi.variant.Header.PrimitiveType.INT8;
import static io.trino.spi.variant.Header.arrayFieldOffsetSize;
import static io.trino.spi.variant.Header.arrayIsLarge;
import static io.trino.spi.variant.Header.getBasicType;
import static io.trino.spi.variant.Header.getPrimitiveType;
import static io.trino.spi.variant.Header.objectFieldIdSize;
import static io.trino.spi.variant.Header.objectFieldOffsetSize;
import static io.trino.spi.variant.Header.objectIsLarge;
import static io.trino.spi.variant.Header.shortStringLength;
import static java.lang.Double.doubleToLongBits;
import static java.lang.Double.doubleToRawLongBits;

public final class VariantUtils
{
    private static final Int128[] INT128_POWERS_OF_FIVE = new Int128[39];
    // |value| >= 1e38 cannot be represented by variant DECIMAL(38, s) for any allowed scale s in [0, 38].
    private static final double DECIMAL128_MAGNITUDE_UPPER_BOUND = 1.0e38;
    private static final Decimal128Canonical ZERO_DECIMAL = new Decimal128Canonical(Int128.ZERO, 0);

    static {
        INT128_POWERS_OF_FIVE[0] = Int128.ONE;
        for (int i = 1; i < INT128_POWERS_OF_FIVE.length; i++) {
            INT128_POWERS_OF_FIVE[i] = Int128Math.multiply(INT128_POWERS_OF_FIVE[i - 1], 5L);
        }
    }

    // Threshold to switch from linear search to binary search for field indexes
    static final int BINARY_SEARCH_THRESHOLD = 64;

    private VariantUtils() {}

    public static void writeOffset(Slice out, int offset, int size, int offsetSize)
    {
        switch (offsetSize) {
            case 1 -> out.setByte(offset, (byte) size);
            case 2 -> out.setShort(offset, (short) size);
            // little endian
            case 3 -> {
                out.setByte(offset, (byte) (size & 0xFF));
                out.setByte(offset + 1, (byte) ((size >> 8) & 0xFF));
                out.setByte(offset + 2, (byte) ((size >> 16) & 0xFF));
            }
            case 4 -> out.setInt(offset, size);
            default -> throw new IllegalStateException("Unsupported offset size: " + offsetSize);
        }
    }

    public static void writeOffset(SliceOutput out, int size, int offsetSize)
    {
        switch (offsetSize) {
            case 1 -> out.writeByte((byte) size);
            case 2 -> out.writeShort((short) size);
            // little endian
            case 3 -> {
                out.writeByte((byte) (size & 0xFF));
                out.writeByte((byte) ((size >> 8) & 0xFF));
                out.writeByte((byte) ((size >> 16) & 0xFF));
            }
            case 4 -> out.writeInt(size);
            default -> throw new IllegalStateException("Unsupported offset size: " + offsetSize);
        }
    }

    public static int readOffset(Slice data, int offset, int size)
    {
        return switch (size) {
            case 1 -> data.getByte(offset) & 0xFF;
            case 2 -> data.getShort(offset) & 0xFFFF;
            // In all current usages, there is an extra byte at the end, so we can read as int directly
            // This method is used for field ids and field offsets. In the case of fieldIds, they are
            // always followed by the offsets, which must have at least one zero offset.  In the case of
            // offsets, the only way to get 3-byte offsets is to have a lot of data. So reading 4 bytes is safe.
            case 3 -> data.getInt(offset) & 0xFFFFFF;
            case 4 -> data.getInt(offset);
            default -> throw new IllegalStateException("Unsupported offset size: " + size);
        };
    }

    public static int readOffset(SliceInput data, int size)
    {
        return switch (size) {
            case 1 -> data.readUnsignedByte();
            case 2 -> data.readUnsignedShort();
            case 3 -> data.readUnsignedShort() |
                    data.readUnsignedByte() << 16;
            case 4 -> data.readInt();
            default -> throw new IllegalStateException("Unsupported offset size: " + size);
        };
    }

    public static int getOffsetSize(int[] offsets)
    {
        return getOffsetSize(offsets[offsets.length - 1]);
    }

    public static int getOffsetSize(int maxOffset)
    {
        if (maxOffset > 0xFFFFFF) {
            return 4;
        }
        else if (maxOffset > 0xFFFF) {
            return 3;
        }
        else if (maxOffset > 0xFF) {
            return 2;
        }
        return 1;
    }

    public static boolean isSorted(Collection<Slice> fieldNames)
    {
        // Iceberg does not consider an empty dictionary as sorted
        if (fieldNames.isEmpty()) {
            return false;
        }

        Iterator<Slice> iterator = fieldNames.iterator();
        if (iterator.hasNext()) {
            Slice previous = iterator.next();
            while (iterator.hasNext()) {
                Slice next = iterator.next();
                if (previous.compareTo(next) > 0) {
                    return false;
                }
                previous = next;
            }
        }
        return true;
    }

    public static int findFieldIndex(Slice fieldName, Metadata metadata, Slice object, int fieldCount, int fieldIdsOffset, int fieldIdSize)
    {
        if (fieldCount > BINARY_SEARCH_THRESHOLD) {
            return binarySearchFieldIndexes(fieldName, metadata, object, fieldCount, fieldIdsOffset, fieldIdSize);
        }

        // linear search
        for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
            if (metadata.get(readOffset(object, fieldIdsOffset + fieldIndex * fieldIdSize, fieldIdSize)).equals(fieldName)) {
                return fieldIndex;
            }
        }
        return -1;
    }

    private static int binarySearchFieldIndexes(Slice fieldName, Metadata metadata, Slice object, int fieldCount, int fieldIdsOffset, int fieldIdSize)
    {
        // fieldIds are sorted, use binary search
        int low = 0;
        int high = fieldCount - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            int midFieldId = readOffset(object, fieldIdsOffset + mid * fieldIdSize, fieldIdSize);
            int compare = metadata.get(midFieldId).compareTo(fieldName);
            if (compare < 0) {
                low = mid + 1;
            }
            else if (compare > 0) {
                high = mid - 1;
            }
            else {
                return mid;
            }
        }
        return -1;
    }

    public static boolean equals(
            Metadata leftMetadata,
            Slice leftSlice,
            int leftOffset,
            Metadata rightMetadata,
            Slice rightSlice,
            int rightOffset)
    {
        if (leftMetadata == rightMetadata && leftSlice == rightSlice && leftOffset == rightOffset) {
            return true;
        }

        byte leftHeader = leftSlice.getByte(leftOffset);
        byte rightHeader = rightSlice.getByte(rightOffset);

        ValueClass leftValueClass = ValueClass.classify(leftHeader);
        ValueClass rightValueClass = ValueClass.classify(rightHeader);

        if (leftValueClass != rightValueClass) {
            return false;
        }

        return switch (leftValueClass) {
            case NULL -> true;
            case BOOLEAN -> leftHeader == rightHeader;
            case NUMERIC -> equalsNumeric(leftHeader, leftSlice, leftOffset, rightHeader, rightSlice, rightOffset);
            case DATE -> leftSlice.getInt(leftOffset + 1) == rightSlice.getInt(rightOffset + 1);
            case TIME_NTZ -> leftSlice.getLong(leftOffset + 1) == rightSlice.getLong(rightOffset + 1);
            case TIMESTAMP_UTC, TIMESTAMP_NTZ -> equalsTimestamp(leftHeader, leftSlice, leftOffset, rightHeader, rightSlice, rightOffset);
            case BINARY -> leftSlice.equals(leftOffset + 5, leftSlice.getInt(leftOffset + 1), rightSlice, rightOffset + 5, rightSlice.getInt(rightOffset + 1));
            case STRING -> equalsStringLike(leftSlice, leftOffset, leftHeader, rightSlice, rightOffset, rightHeader);
            case UUID -> leftSlice.equals(leftOffset + 1, 16, rightSlice, rightOffset + 1, 16);
            case OBJECT -> equalsObject(leftMetadata, leftSlice, leftOffset, rightMetadata, rightSlice, rightOffset);
            case ARRAY -> equalsArray(leftMetadata, leftSlice, leftOffset, rightMetadata, rightSlice, rightOffset);
        };
    }

    private static boolean equalsNumeric(
            byte leftHeader,
            Slice leftSlice,
            int leftOffset,
            byte rightHeader,
            Slice rightSlice,
            int rightOffset)
    {
        PrimitiveType leftType = getPrimitiveType(leftHeader);
        PrimitiveType rightType = getPrimitiveType(rightHeader);

        if (isExactNumeric(leftType) && isExactNumeric(rightType)) {
            return equalsExactNumeric(leftHeader, leftSlice, leftOffset, rightHeader, rightSlice, rightOffset);
        }

        if (isFloatingNumeric(leftType) && isFloatingNumeric(rightType)) {
            return equalsFloatingNumeric(leftType, leftSlice, leftOffset, rightType, rightSlice, rightOffset);
        }

        if (isExactNumeric(leftType)) {
            return equalsExactAndFloatingNumeric(leftType, leftSlice, leftOffset, rightType, rightSlice, rightOffset);
        }
        return equalsExactAndFloatingNumeric(rightType, rightSlice, rightOffset, leftType, leftSlice, leftOffset);
    }

    private static boolean equalsExactNumeric(
            byte leftHeader,
            Slice leftSlice,
            int leftOffset,
            byte rightHeader,
            Slice rightSlice,
            int rightOffset)
    {
        PrimitiveType leftType = getPrimitiveType(leftHeader);
        PrimitiveType rightType = getPrimitiveType(rightHeader);

        // if both sides are integer, compare as long
        if ((leftType == INT8 || leftType == INT16 || leftType == INT32 || leftType == INT64) &&
                (rightType == INT8 || rightType == INT16 || rightType == INT32 || rightType == INT64)) {
            // Both sides are integer
            long leftValue = switch (leftType) {
                case INT8 -> leftSlice.getByte(leftOffset + 1);
                case INT16 -> leftSlice.getShort(leftOffset + 1);
                case INT32 -> leftSlice.getInt(leftOffset + 1);
                case INT64 -> leftSlice.getLong(leftOffset + 1);
                default -> throw new IllegalStateException("Unexpected integer type: " + leftType);
            };
            long rightValue = switch (rightType) {
                case INT8 -> rightSlice.getByte(rightOffset + 1);
                case INT16 -> rightSlice.getShort(rightOffset + 1);
                case INT32 -> rightSlice.getInt(rightOffset + 1);
                case INT64 -> rightSlice.getLong(rightOffset + 1);
                default -> throw new IllegalStateException("Unexpected integer type: " + rightType);
            };
            return leftValue == rightValue;
        }
        return decodeExactNumericCanonical(leftType, leftSlice, leftOffset)
                .equals(decodeExactNumericCanonical(rightType, rightSlice, rightOffset));
    }

    @SuppressWarnings("FloatingPointEquality")
    private static boolean equalsFloatingNumeric(
            PrimitiveType leftType,
            Slice leftSlice,
            int leftOffset,
            PrimitiveType rightType,
            Slice rightSlice,
            int rightOffset)
    {
        double leftValue = floatingAsDouble(leftType, leftSlice, leftOffset);
        double rightValue = floatingAsDouble(rightType, rightSlice, rightOffset);

        if (Double.isNaN(leftValue) || Double.isNaN(rightValue)) {
            return false;
        }

        return leftValue == rightValue;
    }

    private static boolean equalsExactAndFloatingNumeric(
            PrimitiveType exactType,
            Slice exactSlice,
            int exactOffset,
            PrimitiveType floatingType,
            Slice floatingSlice,
            int floatingOffset)
    {
        double floatingValue = floatingAsDouble(floatingType, floatingSlice, floatingOffset);
        if (!Double.isFinite(floatingValue)) {
            return false;
        }

        // If double can be exactly represented as a long, use simpler form
        // The explicit check for zero is required because -0.0d is not an exact conversion
        if (floatingValue == 0.0 || ExactConversionsSupport.isDoubleToLongExact(floatingValue)) {
            return equalsExactNumericWithLong(exactType, exactSlice, exactOffset, (long) floatingValue);
        }

        Decimal128Canonical floatingDecimal = tryToDecimal128Exact(floatingValue);
        if (floatingDecimal == null) {
            return false;
        }

        Decimal128Canonical exactDecimal = decodeExactNumericCanonical(exactType, exactSlice, exactOffset);
        return exactDecimal.equals(floatingDecimal);
    }

    private static boolean equalsExactNumericWithLong(PrimitiveType exactType, Slice exactSlice, int exactOffset, long value)
    {
        return switch (exactType) {
            case INT8 -> exactSlice.getByte(exactOffset + 1) == value;
            case INT16 -> exactSlice.getShort(exactOffset + 1) == value;
            case INT32 -> exactSlice.getInt(exactOffset + 1) == value;
            case INT64 -> exactSlice.getLong(exactOffset + 1) == value;
            case DECIMAL4 -> decimalEqualsLong(exactSlice.getInt(exactOffset + 2), exactSlice.getByte(exactOffset + 1) & 0xFF, value);
            case DECIMAL8 -> decimalEqualsLong(exactSlice.getLong(exactOffset + 2), exactSlice.getByte(exactOffset + 1) & 0xFF, value);
            case DECIMAL16 -> decimalEqualsLong(
                    Int128.valueOf(exactSlice.getLong(exactOffset + 10), exactSlice.getLong(exactOffset + 2)),
                    exactSlice.getByte(exactOffset + 1) & 0xFF,
                    value);
            default -> throw new IllegalStateException("Unexpected exact numeric type: " + exactType);
        };
    }

    private static boolean decimalEqualsLong(long unscaled, int scale, long value)
    {
        if (scale == 0) {
            return unscaled == value;
        }
        if (unscaled == 0 || value == 0) {
            return unscaled == 0 && value == 0;
        }
        if ((unscaled < 0) != (value < 0)) {
            return false;
        }
        if (scale > 18) {
            // Any non-zero value * 10^scale is out of long range for scale > 18.
            return false;
        }
        try {
            // For decimal(unscaled, scale), numeric equality with an integer long requires:
            // unscaled == value * 10^scale.
            // For scale <= 18, 10^scale fits in long, so we can do a single exact long multiply.
            return unscaled == Math.multiplyExact(value, longTenToNth(scale));
        }
        catch (ArithmeticException ignored) {
            return false;
        }
    }

    private static boolean decimalEqualsLong(Int128 unscaled, int scale, long value)
    {
        if (scale == 0) {
            return unscaled.getHigh() == (value >> 63) && unscaled.getLow() == value;
        }
        if (unscaled.isZero() || value == 0) {
            return unscaled.isZero() && value == 0;
        }
        if (unscaled.isNegative() != (value < 0)) {
            return false;
        }

        if (scale <= 18) {
            try {
                // decimal(unscaled, scale) is numerically equal to long(value) iff:
                // unscaled / 10^scale == value  <=>  unscaled == value * 10^scale.
                // For scale <= 18, 10^scale fits in long, so we can compute that exact multiple
                // cheaply in 64-bit math and then compare with sign-extended Int128 limbs.
                long scaled = Math.multiplyExact(value, longTenToNth(scale));
                return unscaled.getHigh() == (scaled >> 63) && unscaled.getLow() == scaled;
            }
            catch (ArithmeticException ignored) {
                return false;
            }
        }

        Int128 powerOfTen = Int128Math.powerOfTen(scale);
        long[] scaledValue = new long[2];
        try {
            // Same equality test as above, but for larger scales where value * 10^scale no longer
            // fits in long. We compute the exact 128-bit two's-complement product and compare limbs:
            // [unscaledHigh, unscaledLow] == [scaledValueHigh, scaledValueLow].
            // If multiply overflows 128 bits, equality cannot hold for a DECIMAL16 payload.
            Int128Math.multiply(value >> 63, value, powerOfTen.getHigh(), powerOfTen.getLow(), scaledValue, 0);
        }
        catch (ArithmeticException ignored) {
            return false;
        }
        return unscaled.getHigh() == scaledValue[0] && unscaled.getLow() == scaledValue[1];
    }

    private static boolean isExactNumeric(PrimitiveType primitiveType)
    {
        return switch (primitiveType) {
            case INT8, INT16, INT32, INT64, DECIMAL4, DECIMAL8, DECIMAL16 -> true;
            default -> false;
        };
    }

    private static boolean isFloatingNumeric(PrimitiveType primitiveType)
    {
        return primitiveType == FLOAT || primitiveType == DOUBLE;
    }

    private static double floatingAsDouble(PrimitiveType primitiveType, Slice slice, int offset)
    {
        return switch (primitiveType) {
            case FLOAT -> slice.getFloat(offset + 1);
            case DOUBLE -> slice.getDouble(offset + 1);
            default -> throw new IllegalStateException("Unexpected floating type: " + primitiveType);
        };
    }

    private static Decimal128Canonical decodeExactNumericCanonical(PrimitiveType primitiveType, Slice slice, int offset)
    {
        return switch (primitiveType) {
            case INT8 -> new Decimal128Canonical(Int128.valueOf(slice.getByte(offset + 1)), 0);
            case INT16 -> new Decimal128Canonical(Int128.valueOf(slice.getShort(offset + 1)), 0);
            case INT32 -> new Decimal128Canonical(Int128.valueOf(slice.getInt(offset + 1)), 0);
            case INT64 -> new Decimal128Canonical(Int128.valueOf(slice.getLong(offset + 1)), 0);
            case DECIMAL4 -> canonicalizeDecimal(slice.getInt(offset + 2), slice.getByte(offset + 1) & 0xFF);
            case DECIMAL8 -> canonicalizeDecimal(slice.getLong(offset + 2), slice.getByte(offset + 1) & 0xFF);
            case DECIMAL16 -> canonicalizeDecimal(
                    Int128.valueOf(slice.getLong(offset + 10), slice.getLong(offset + 2)),
                    slice.getByte(offset + 1) & 0xFF);
            default -> throw new IllegalStateException("Unexpected exact numeric type: " + primitiveType);
        };
    }

    private static Decimal128Canonical canonicalizeDecimal(long unscaled, int scale)
    {
        if (unscaled == 0) {
            return ZERO_DECIMAL;
        }
        while (scale > 0 && unscaled % 10 == 0) {
            unscaled /= 10;
            scale--;
        }
        return new Decimal128Canonical(Int128.valueOf(unscaled), scale);
    }

    private static Decimal128Canonical canonicalizeDecimal(Int128 unscaled, int scale)
    {
        if (unscaled.isZero()) {
            return ZERO_DECIMAL;
        }
        if (fitsInLong(unscaled)) {
            return canonicalizeDecimal(unscaled.getLow(), scale);
        }

        long currentHigh = unscaled.getHigh();
        long currentLow = unscaled.getLow();
        long[] quotient = new long[2];
        long[] product = new long[2];
        while (scale > 0) {
            if (!tryDivideByTenExactly(currentHigh, currentLow, quotient, product)) {
                break;
            }
            currentHigh = quotient[0];
            currentLow = quotient[1];
            scale--;
        }
        return new Decimal128Canonical(Int128.valueOf(currentHigh, currentLow), scale);
    }

    private static Decimal128Canonical tryToDecimal128Exact(double value)
    {
        if (value == 0.0) {
            // Handles both +0.0 and -0.0 and avoids zero-specific exponent/significand corner cases.
            return ZERO_DECIMAL;
        }
        if (!Double.isFinite(value) || !canFitInVariantDecimal128(value)) {
            return null;
        }

        // Decode IEEE-754 double bit fields:
        // sign bit (bit 63), exponent field (bits 62..52), fraction field (bits 51..0).
        long bits = doubleToRawLongBits(value);
        boolean negative = bits < 0;
        int exponentBits = (int) ((bits >>> 52) & 0x7FFL);
        long fractionBits = bits & ((1L << 52) - 1);

        // Re-express the value as: signedInteger * 2^binaryExponent, where signedInteger is an exact integer.
        long significand;
        int binaryExponent;
        if (exponentBits == 0) {
            // Subnormal numbers have no implicit leading 1 bit.
            significand = fractionBits;
            binaryExponent = -1074;
        }
        else {
            // Normalized numbers have an implicit leading 1 bit before the fraction bits.
            significand = (1L << 52) | fractionBits;
            binaryExponent = exponentBits - 1075;
        }

        // Pull out all factors of 2 from the integer part.
        // This minimizes the eventual decimal scale because every removed factor of 2 lets us
        // replace one "divide by 2" in the exponent with one "multiply by 5" in the unscaled value:
        // (integer * 2^-k) == ((integer / 2^t) * 5^(k-t)) * 10^-(k-t), for maximal t.
        int trailingZeroBits = Long.numberOfTrailingZeros(significand);
        significand >>>= trailingZeroBits;
        binaryExponent += trailingZeroBits;

        Int128 unscaled;
        int scale;
        if (binaryExponent >= 0) {
            // Value is already an integer after applying the remaining power-of-two exponent:
            // value = significand * 2^binaryExponent, so decimal scale is 0.
            scale = 0;
            unscaled = tryShiftLeftToPositiveInt128(significand, binaryExponent);
            if (unscaled == null) {
                return null;
            }
        }
        else {
            // We still have a negative binary exponent: value = significand / 2^(-binaryExponent).
            // Convert to decimal form by multiplying by 5^scale and setting decimal scale=scale:
            // significand / 2^scale == (significand * 5^scale) / 10^scale.
            scale = -binaryExponent;
            if (scale > 38) {
                return null;
            }
            try {
                unscaled = Int128Math.multiply(Int128.valueOf(significand), INT128_POWERS_OF_FIVE[scale]);
            }
            catch (ArithmeticException ignored) {
                return null;
            }
        }

        if (negative) {
            unscaled = Int128Math.negate(unscaled);
        }
        return canonicalizeDecimal(unscaled, scale);
    }

    private static boolean canFitInVariantDecimal128(double value)
    {
        return Math.abs(value) < DECIMAL128_MAGNITUDE_UPPER_BOUND;
    }

    private static boolean tryDivideByTenExactly(long high, long low, long[] quotient, long[] product)
    {
        // Compute quotient once, then verify exactness via multiply-back.
        Int128Math.rescaleTruncate(high, low, -1, quotient, 0);
        Int128Math.multiply(quotient[0], quotient[1], 0, 10L, product, 0);
        return product[0] == high && product[1] == low;
    }

    private static Int128 tryShiftLeftToPositiveInt128(long value, int shift)
    {
        if (shift >= 128) {
            return null;
        }

        long high;
        long low;
        if (shift == 0) {
            high = 0;
            low = value;
        }
        else if (shift < 64) {
            high = value >>> (64 - shift);
            low = value << shift;
        }
        else {
            high = value << (shift - 64);
            low = 0;
        }

        // Positive Int128 values must keep the sign bit (bit 127) clear.
        if (high < 0) {
            return null;
        }
        return Int128.valueOf(high, low);
    }

    private static boolean fitsInLong(Int128 value)
    {
        return value.getHigh() == (value.getLow() >> 63);
    }

    private record Decimal128Canonical(Int128 unscaled, int scale)
    {
        private Decimal128Canonical
        {
            checkArgument(scale >= 0 && scale <= 38, () -> "Invalid decimal scale: %s".formatted(scale));
        }
    }

    private static boolean equalsTimestamp(
            byte leftHeader, Slice leftSlice, int leftOffset,
            byte rightHeader, Slice rightSlice, int rightOffset)
    {
        long leftValue = leftSlice.getLong(leftOffset + 1);
        long rightValue = rightSlice.getLong(rightOffset + 1);

        if (leftHeader == rightHeader) {
            return leftValue == rightValue;
        }

        // One side micros, one side nanos
        PrimitiveType leftType = getPrimitiveType(leftHeader);
        if (leftType == PrimitiveType.TIMESTAMP_UTC_MICROS || leftType == PrimitiveType.TIMESTAMP_NTZ_MICROS) {
            // left micros, right nanos
            return rightValue % 1_000L == 0 && leftValue == (rightValue / 1_000L);
        }
        // left nanos, right micros
        return leftValue % 1_000L == 0 && (leftValue / 1_000L) == rightValue;
    }

    private static boolean equalsStringLike(
            Slice leftSlice, int leftOffset, byte leftHeader,
            Slice rightSlice, int rightOffset, byte rightHeader)
    {
        int leftStringOffset;
        int leftStringLength;
        if (getBasicType(leftHeader) == SHORT_STRING) {
            leftStringLength = shortStringLength(leftHeader);
            leftStringOffset = leftOffset + 1;
        }
        else {
            // PRIMITIVE STRING: [header][int length][bytes...]
            leftStringLength = leftSlice.getInt(leftOffset + 1);
            leftStringOffset = leftOffset + 5;
        }

        int rightStringOffset;
        int rightStringLength;
        if (getBasicType(rightHeader) == SHORT_STRING) {
            rightStringLength = shortStringLength(rightHeader);
            rightStringOffset = rightOffset + 1;
        }
        else {
            rightStringLength = rightSlice.getInt(rightOffset + 1);
            rightStringOffset = rightOffset + 5;
        }

        return leftSlice.equals(leftStringOffset, leftStringLength, rightSlice, rightStringOffset, rightStringLength);
    }

    private static boolean equalsObject(
            Metadata leftMetadata, Slice leftSlice, int leftOffset,
            Metadata rightMetadata, Slice rightSlice, int rightOffset)
    {
        byte leftHeader = leftSlice.getByte(leftOffset);
        byte rightHeader = rightSlice.getByte(rightOffset);

        boolean leftLarge = objectIsLarge(leftHeader);
        boolean rightLarge = objectIsLarge(rightHeader);

        int leftCount = leftLarge ? leftSlice.getInt(leftOffset + 1) : (leftSlice.getByte(leftOffset + 1) & 0xFF);
        int rightCount = rightLarge ? rightSlice.getInt(rightOffset + 1) : (rightSlice.getByte(rightOffset + 1) & 0xFF);
        if (leftCount != rightCount) {
            return false;
        }

        int leftIdSize = objectFieldIdSize(leftHeader);
        int leftFieldOffsetSize = objectFieldOffsetSize(leftHeader);

        int rightIdSize = objectFieldIdSize(rightHeader);
        int rightFieldOffsetSize = objectFieldOffsetSize(rightHeader);

        int leftIdsStart = leftOffset + 1 + (leftLarge ? 4 : 1);
        int rightIdsStart = rightOffset + 1 + (rightLarge ? 4 : 1);

        int leftOffsetsStart = leftIdsStart + leftCount * leftIdSize;
        int rightOffsetsStart = rightIdsStart + rightCount * rightIdSize;

        int leftValuesStart = leftOffsetsStart + (leftCount + 1) * leftFieldOffsetSize;
        int rightValuesStart = rightOffsetsStart + (rightCount + 1) * rightFieldOffsetSize;

        // First pass: compare keys only
        if (leftMetadata == rightMetadata) {
            for (int index = 0; index < leftCount; index++) {
                int leftFieldId = readOffset(leftSlice, leftIdsStart + index * leftIdSize, leftIdSize);
                int rightFieldId = readOffset(rightSlice, rightIdsStart + index * rightIdSize, rightIdSize);
                if (leftFieldId != rightFieldId) {
                    return false;
                }
            }
        }
        else {
            for (int index = 0; index < leftCount; index++) {
                int leftFieldId = readOffset(leftSlice, leftIdsStart + index * leftIdSize, leftIdSize);
                int rightFieldId = readOffset(rightSlice, rightIdsStart + index * rightIdSize, rightIdSize);

                Slice leftKey = leftMetadata.get(leftFieldId);
                Slice rightKey = rightMetadata.get(rightFieldId);

                if (!leftKey.equals(rightKey)) {
                    return false;
                }
            }
        }

        // Second pass: compare values (only once we know the key sets match)
        for (int index = 0; index < leftCount; index++) {
            int leftValueStart = leftValuesStart + readOffset(leftSlice, leftOffsetsStart + index * leftFieldOffsetSize, leftFieldOffsetSize);
            int rightValueStart = rightValuesStart + readOffset(rightSlice, rightOffsetsStart + index * rightFieldOffsetSize, rightFieldOffsetSize);

            if (!equals(leftMetadata, leftSlice, leftValueStart, rightMetadata, rightSlice, rightValueStart)) {
                return false;
            }
        }

        return true;
    }

    private static boolean equalsArray(
            Metadata leftMetadata, Slice leftSlice, int leftOffset,
            Metadata rightMetadata, Slice rightSlice, int rightOffset)
    {
        byte leftHeader = leftSlice.getByte(leftOffset);
        byte rightHeader = rightSlice.getByte(rightOffset);

        boolean leftLarge = arrayIsLarge(leftHeader);
        boolean rightLarge = arrayIsLarge(rightHeader);

        int leftCount = leftLarge ? leftSlice.getInt(leftOffset + 1) : (leftSlice.getByte(leftOffset + 1) & 0xFF);
        int rightCount = rightLarge ? rightSlice.getInt(rightOffset + 1) : (rightSlice.getByte(rightOffset + 1) & 0xFF);
        if (leftCount != rightCount) {
            return false;
        }

        int leftOffsetSize = arrayFieldOffsetSize(leftHeader);
        int rightOffsetSize = arrayFieldOffsetSize(rightHeader);

        int leftOffsetsStart = leftOffset + 1 + (leftLarge ? 4 : 1);
        int rightOffsetsStart = rightOffset + 1 + (rightLarge ? 4 : 1);

        int leftValuesStart = leftOffsetsStart + (leftCount + 1) * leftOffsetSize;
        int rightValuesStart = rightOffsetsStart + (rightCount + 1) * rightOffsetSize;

        for (int index = 0; index < leftCount; index++) {
            int leftElementStart = leftValuesStart + readOffset(leftSlice, leftOffsetsStart + index * leftOffsetSize, leftOffsetSize);
            int rightElementStart = rightValuesStart + readOffset(rightSlice, rightOffsetsStart + index * rightOffsetSize, rightOffsetSize);

            if (!equals(leftMetadata, leftSlice, leftElementStart, rightMetadata, rightSlice, rightElementStart)) {
                return false;
            }
        }
        return true;
    }

    public static long hashCode(Metadata metadata, Slice slice, int offset)
    {
        VariantHash variantHash = new VariantHash(0);
        variantHash.hashVariant(metadata, slice, offset);
        return variantHash.finish();
    }

    private static final class VariantHash
    {
        private static final long PRIME64_1 = 0x9E3779B185EBCA87L;
        private static final long PRIME64_2 = 0xC2B2AE3D27D4EB4FL;
        private static final long PRIME64_3 = 0x165667B19E3779F9L;
        private static final long PRIME64_4 = 0x85EBCA77C2B2AE63L;
        private static final long PRIME64_5 = 0x27D4EB2F165667C5L;

        private long hash;
        private long totalLength; // in bytes, for finalization

        VariantHash(long seed)
        {
            // “small input” init form from xxHash64
            this.hash = seed + PRIME64_5;
        }

        public void hashVariant(Metadata metadata, Slice slice, int offset)
        {
            byte header = slice.getByte(offset);
            ValueClass valueClass = ValueClass.classify(header);
            addInt(valueClass.hashTag());
            switch (valueClass) {
                case NULL -> addLong(0);
                case BOOLEAN -> addLong(getPrimitiveType(header) == PrimitiveType.BOOLEAN_TRUE ? 1 : 2);
                case NUMERIC -> hashNumeric(getPrimitiveType(header), slice, offset);
                case DATE -> addInt(slice.getInt(offset + 1));
                case TIME_NTZ -> addLong(slice.getLong(offset + 1));
                case TIMESTAMP_UTC, TIMESTAMP_NTZ -> hashTimestamp(header, slice, offset);
                case BINARY -> addBytesHash(slice, offset + 5, slice.getInt(offset + 1));
                case STRING -> hashStringLike(slice, offset, header);
                case UUID -> addBytesHash(slice, offset + 1, 16);
                case OBJECT -> hashObject(metadata, slice, offset);
                case ARRAY -> hashArray(metadata, slice, offset);
            }
        }

        private void hashNumeric(PrimitiveType primitiveType, Slice slice, int offset)
        {
            if (isExactNumeric(primitiveType)) {
                hashCanonicalDecimal(decodeExactNumericCanonical(primitiveType, slice, offset));
                return;
            }

            double value = floatingAsDouble(primitiveType, slice, offset);
            if (Double.isNaN(value)) {
                addInt(4);
                return;
            }
            if (Double.isInfinite(value)) {
                addInt(value > 0 ? 2 : 3);
                return;
            }
            // Canonicalize both +0.0 and -0.0 to the long-zero hash.
            // For other values, take the same long-canonical path when conversion is exact.
            if (value == 0.0 || ExactConversionsSupport.isDoubleToLongExact(value)) {
                hashCanonicalLongDecimal((long) value);
                return;
            }

            Decimal128Canonical decimal = tryToDecimal128Exact(value);
            if (decimal != null) {
                hashCanonicalDecimal(decimal);
                return;
            }

            addInt(1);
            addDouble(value);
        }

        private void hashCanonicalDecimal(Decimal128Canonical decimal)
        {
            addInt(0);
            if (decimal.scale() != 0) {
                addInt(decimal.scale());
            }
            Int128 unscaled = decimal.unscaled();
            if (fitsInLong(unscaled)) {
                addLong(unscaled.getLow());
                return;
            }
            addLong(unscaled.getHigh());
            addLong(unscaled.getLow());
        }

        private void hashCanonicalLongDecimal(long value)
        {
            addInt(0);
            addLong(value);
        }

        private void addInt(int value)
        {
            addLong(value);
        }

        private void addLong(long value)
        {
            totalLength += Long.BYTES;
            hash ^= round(value);
            hash = Long.rotateLeft(hash, 27) * PRIME64_1 + PRIME64_4;
        }

        private void addDouble(double value)
        {
            addLong(doubleToLongBits(value));
        }

        void addBytesHash(Slice slice, int offset, int length)
        {
            long bytesXxHash = XxHash64.hash(slice, offset, length);
            addBytesHash(bytesXxHash, length);
        }

        void addBytesHash(long bytesXxHash, int length)
        {
            totalLength += length;
            hash ^= round(bytesXxHash);
            hash = Long.rotateLeft(hash, 27) * PRIME64_1 + PRIME64_4;
        }

        private void hashStringLike(Slice slice, int offset, byte header)
        {
            int stringOffset;
            int stringLength;
            if (getBasicType(header) == SHORT_STRING) {
                stringLength = shortStringLength(header);
                stringOffset = offset + 1;
            }
            else {
                stringLength = slice.getInt(offset + 1);
                stringOffset = offset + 5;
            }

            addBytesHash(slice, stringOffset, stringLength);
        }

        private void hashTimestamp(byte header, Slice slice, int offset)
        {
            long value = slice.getLong(offset + 1);

            PrimitiveType primitiveType = getPrimitiveType(header);
            if (primitiveType == PrimitiveType.TIMESTAMP_UTC_MICROS || primitiveType == PrimitiveType.TIMESTAMP_NTZ_MICROS) {
                // micros marker, so that nanos and micros do not collide
                addLong(0);
                addLong(value);
                return;
            }

            // Canonicalize nanos that are exactly representable in micros to micros
            if (value % 1_000L == 0) {
                // micros marker, so that nanos and micros do not collide
                addLong(0);
                addLong(value / 1_000L);
                return;
            }
            // nanos marker
            addLong(1);
            addLong(value);
        }

        private void hashObject(Metadata metadata, Slice slice, int offset)
        {
            byte header = slice.getByte(offset);
            boolean large = objectIsLarge(header);
            int count = large ? slice.getInt(offset + 1) : (slice.getByte(offset + 1) & 0xFF);

            int idSize = objectFieldIdSize(header);
            int fieldOffsetSize = objectFieldOffsetSize(header);

            int idsStart = offset + 1 + (large ? 4 : 1);
            int offsetsStart = idsStart + count * idSize;
            int valuesStart = offsetsStart + (count + 1) * fieldOffsetSize;

            addInt(count);
            for (int index = 0; index < count; index++) {
                int fieldId = readOffset(slice, idsStart + index * idSize, idSize);
                Slice key = metadata.get(fieldId);

                int valueStart = valuesStart + readOffset(slice, offsetsStart + index * fieldOffsetSize, fieldOffsetSize);
                addBytesHash(key, 0, key.length());
                hashVariant(metadata, slice, valueStart);
            }
        }

        private void hashArray(Metadata metadata, Slice slice, int offset)
        {
            byte header = slice.getByte(offset);
            boolean large = arrayIsLarge(header);
            int count = large ? slice.getInt(offset + 1) : (slice.getByte(offset + 1) & 0xFF);

            int offsetSize = arrayFieldOffsetSize(header);
            int offsetsStart = offset + 1 + (large ? 4 : 1);
            int valuesStart = offsetsStart + (count + 1) * offsetSize;

            addInt(count);
            for (int index = 0; index < count; index++) {
                int elementStart = valuesStart + readOffset(slice, offsetsStart + index * offsetSize, offsetSize);
                hashVariant(metadata, slice, elementStart);
            }
        }

        long finish()
        {
            long h = hash + totalLength;

            // xxHash64 avalanche
            h ^= h >>> 33;
            h *= PRIME64_2;
            h ^= h >>> 29;
            h *= PRIME64_3;
            h ^= h >>> 32;
            return h;
        }

        private static long round(long value)
        {
            return Long.rotateLeft(value * PRIME64_2, 31) * PRIME64_1;
        }
    }

    private enum ValueClass
    {
        NULL(0),
        BOOLEAN(1),
        NUMERIC(2),
        DATE(5),
        TIME_NTZ(6),
        TIMESTAMP_UTC(7),
        TIMESTAMP_NTZ(8),
        BINARY(9),
        STRING(10),
        UUID(11),
        OBJECT(12),
        ARRAY(13);

        private final int hashTag;

        ValueClass(int hashTag)
        {
            this.hashTag = hashTag;
        }

        private int hashTag()
        {
            return hashTag;
        }

        private static ValueClass classify(byte header)
        {
            return switch (getBasicType(header)) {
                case PRIMITIVE -> switch (getPrimitiveType(header)) {
                    case NULL -> NULL;
                    case BOOLEAN_TRUE, BOOLEAN_FALSE -> BOOLEAN;
                    case INT8, INT16, INT32, INT64, DECIMAL4, DECIMAL8, DECIMAL16, FLOAT, DOUBLE -> NUMERIC;
                    case DATE -> DATE;
                    case TIME_NTZ_MICROS -> TIME_NTZ;
                    case TIMESTAMP_UTC_MICROS, TIMESTAMP_UTC_NANOS -> TIMESTAMP_UTC;
                    case TIMESTAMP_NTZ_MICROS, TIMESTAMP_NTZ_NANOS -> TIMESTAMP_NTZ;
                    case BINARY -> BINARY;
                    case STRING -> STRING;
                    case UUID -> UUID;
                };
                case SHORT_STRING -> STRING;
                case OBJECT -> OBJECT;
                case ARRAY -> ARRAY;
            };
        }
    }

    static void checkArgument(boolean test, String message)
    {
        if (!test) {
            throw new IllegalArgumentException(message);
        }
    }

    static void checkArgument(boolean test, Supplier<String> message)
    {
        if (!test) {
            throw new IllegalArgumentException(message.get());
        }
    }

    static void checkState(boolean test, String message)
    {
        if (!test) {
            throw new IllegalStateException(message);
        }
    }

    static void checkState(boolean test, Supplier<String> message)
    {
        if (!test) {
            throw new IllegalStateException(message.get());
        }
    }

    static void verify(boolean test, String message)
    {
        if (!test) {
            throw new VerifyException(message);
        }
    }

    private static class VerifyException
            extends RuntimeException
    {
        public VerifyException(String message)
        {
            super(message);
        }
    }
}
