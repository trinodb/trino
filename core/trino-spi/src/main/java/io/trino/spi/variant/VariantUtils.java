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
import io.trino.spi.type.Int128;
import io.trino.spi.type.Int128Math;

import java.util.Collection;
import java.util.Iterator;
import java.util.function.Supplier;

import static io.trino.spi.variant.Header.PrimitiveType;
import static io.trino.spi.variant.Header.PrimitiveType.DOUBLE;
import static io.trino.spi.variant.Header.PrimitiveType.FLOAT;
import static io.trino.spi.variant.Header.PrimitiveType.INT16;
import static io.trino.spi.variant.Header.PrimitiveType.INT32;
import static io.trino.spi.variant.Header.PrimitiveType.INT64;
import static io.trino.spi.variant.Header.PrimitiveType.INT8;
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
            default -> throw new IllegalArgumentException("Unsupported offset size: " + offsetSize);
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
            default -> throw new IllegalArgumentException("Unsupported offset size: " + offsetSize);
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
            default -> throw new IllegalArgumentException("Unsupported offset size: " + size);
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
            default -> throw new IllegalArgumentException("Unsupported offset size: " + size);
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
        return VariantEquality.equals(leftMetadata, leftSlice, leftOffset, rightMetadata, rightSlice, rightOffset);
    }

    static boolean isExactNumeric(PrimitiveType primitiveType)
    {
        return switch (primitiveType) {
            case INT8, INT16, INT32, INT64, DECIMAL4, DECIMAL8, DECIMAL16 -> true;
            default -> false;
        };
    }

    static boolean isFloatingNumeric(PrimitiveType primitiveType)
    {
        return primitiveType == FLOAT || primitiveType == DOUBLE;
    }

    static double floatingAsDouble(PrimitiveType primitiveType, Slice slice, int offset)
    {
        return switch (primitiveType) {
            case FLOAT -> slice.getFloat(offset + 1);
            case DOUBLE -> slice.getDouble(offset + 1);
            default -> throw new VerifyException("Unexpected floating type: " + primitiveType);
        };
    }

    static Decimal128Canonical decodeExactNumericCanonical(PrimitiveType primitiveType, Slice slice, int offset)
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
            default -> throw new VerifyException("Unexpected exact numeric type: " + primitiveType);
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

    static Decimal128Canonical tryToDecimal128Exact(double value)
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

    static boolean fitsInLong(Int128 value)
    {
        return value.getHigh() == (value.getLow() >> 63);
    }

    static record Decimal128Canonical(Int128 unscaled, int scale)
    {
        Decimal128Canonical
        {
            checkArgument(scale >= 0 && scale <= 38, () -> "Invalid decimal scale: %s".formatted(scale));
        }
    }

    public static long hashCode(Metadata metadata, Slice slice, int offset)
    {
        return VariantHashing.hashCode(metadata, slice, offset);
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
}
