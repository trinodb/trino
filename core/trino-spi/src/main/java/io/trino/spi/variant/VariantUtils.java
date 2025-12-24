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
import io.trino.spi.variant.Header.EquivalenceClass;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Supplier;

import static io.trino.spi.variant.Header.BasicType.SHORT_STRING;
import static io.trino.spi.variant.Header.PrimitiveType;
import static io.trino.spi.variant.Header.PrimitiveType.DECIMAL16;
import static io.trino.spi.variant.Header.PrimitiveType.DECIMAL4;
import static io.trino.spi.variant.Header.PrimitiveType.INT16;
import static io.trino.spi.variant.Header.PrimitiveType.INT32;
import static io.trino.spi.variant.Header.PrimitiveType.INT64;
import static io.trino.spi.variant.Header.PrimitiveType.INT8;
import static io.trino.spi.variant.Header.arrayFieldOffsetSize;
import static io.trino.spi.variant.Header.arrayIsLarge;
import static io.trino.spi.variant.Header.getBasicType;
import static io.trino.spi.variant.Header.getEquivalenceClass;
import static io.trino.spi.variant.Header.getPrimitiveType;
import static io.trino.spi.variant.Header.objectFieldIdSize;
import static io.trino.spi.variant.Header.objectFieldOffsetSize;
import static io.trino.spi.variant.Header.objectIsLarge;
import static io.trino.spi.variant.Header.shortStringLength;
import static java.lang.Double.doubleToLongBits;
import static java.lang.Math.pow;
import static java.lang.Math.round;

public final class VariantUtils
{
    private static final VarHandle BIG_ENDIAN_LONG_VIEW = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);
    private static final long[] LONG_POWERS_OF_TEN = new long[19];

    static {
        for (int i = 0; i < LONG_POWERS_OF_TEN.length; ++i) {
            // Although this computes using doubles, incidentally, this is exact for all powers of 10 that fit in a long.
            LONG_POWERS_OF_TEN[i] = round(pow(10, i));
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
            case 1 -> data.readByte();
            case 2 -> data.readShort();
            case 3 -> data.readShort() & 0xFFFF |
                    (data.readByte() & 0xFF) << 16;
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

    @SuppressWarnings("FloatingPointEquality")
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

        EquivalenceClass leftEquivalenceClass = getEquivalenceClass(leftHeader);
        EquivalenceClass rightEquivalenceClass = getEquivalenceClass(rightHeader);

        if (leftEquivalenceClass != rightEquivalenceClass) {
            return false;
        }

        return switch (leftEquivalenceClass) {
            case NULL -> true;
            case BOOLEAN -> leftHeader == rightHeader;
            case EXACT_NUMERIC -> equalsExactNumeric(leftHeader, leftSlice, leftOffset, rightHeader, rightSlice, rightOffset);
            case FLOAT -> leftSlice.getFloat(leftOffset + 1) == rightSlice.getFloat(rightOffset + 1);
            case DOUBLE -> leftSlice.getDouble(leftOffset + 1) == rightSlice.getDouble(rightOffset + 1);
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

    private static boolean equalsExactNumeric(
            byte leftHeader, Slice leftSlice, int leftOffset,
            byte rightHeader, Slice rightSlice, int rightOffset)
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

        // for decimal 4 an 8, compare as long with rescaling
        if (leftType != DECIMAL16 && rightType != DECIMAL16) {
            long leftUnscaled;
            int leftScale = 0;
            switch (leftType) {
                case INT8 -> leftUnscaled = leftSlice.getByte(leftOffset + 1);
                case INT16 -> leftUnscaled = leftSlice.getShort(leftOffset + 1);
                case INT32 -> leftUnscaled = leftSlice.getInt(leftOffset + 1);
                case INT64 -> leftUnscaled = leftSlice.getLong(leftOffset + 1);
                case DECIMAL4 -> {
                    leftScale = leftSlice.getByte(leftOffset + 1);
                    leftUnscaled = leftSlice.getInt(leftOffset + 2);
                }
                case DECIMAL8 -> {
                    leftScale = leftSlice.getByte(leftOffset + 1);
                    leftUnscaled = leftSlice.getLong(leftOffset + 2);
                }
                default -> throw new IllegalStateException("Unexpected left type: " + leftType);
            }

            long rightUnscaled;
            int rightScale = 0;
            switch (rightType) {
                case INT8 -> rightUnscaled = rightSlice.getByte(rightOffset + 1);
                case INT16 -> rightUnscaled = rightSlice.getShort(rightOffset + 1);
                case INT32 -> rightUnscaled = rightSlice.getInt(rightOffset + 1);
                case INT64 -> rightUnscaled = rightSlice.getLong(rightOffset + 1);
                case DECIMAL4 -> {
                    rightScale = rightSlice.getByte(rightOffset + 1);
                    rightUnscaled = rightSlice.getInt(rightOffset + 2);
                }
                case DECIMAL8 -> {
                    rightScale = rightSlice.getByte(rightOffset + 1);
                    rightUnscaled = rightSlice.getLong(rightOffset + 2);
                }
                default -> throw new IllegalStateException("Unexpected right type: " + rightType);
            }

            // zero is equal regardless of scale
            if (leftUnscaled == 0 || rightUnscaled == 0) {
                return leftUnscaled == rightUnscaled;
            }

            // simple fast-path: same scale and same low/high parts
            if (leftScale == rightScale) {
                return leftUnscaled == rightUnscaled;
            }
            if (leftScale < rightScale) {
                int scaleDiff = rightScale - leftScale;
                // left and right cannot be equal if scale difference is too large
                if (scaleDiff >= LONG_POWERS_OF_TEN.length) {
                    return false;
                }
                long rescaleSize = LONG_POWERS_OF_TEN[scaleDiff];
                if (rightUnscaled % rescaleSize != 0) {
                    return false;
                }
                return leftUnscaled == (rightUnscaled / rescaleSize);
            }
            int scaleDiff = leftScale - rightScale;
            // left and right cannot be equal if scale difference is too large
            if (scaleDiff >= LONG_POWERS_OF_TEN.length) {
                return false;
            }
            long rescaleSize = LONG_POWERS_OF_TEN[scaleDiff];
            if (leftUnscaled % rescaleSize != 0) {
                return false;
            }
            return (leftUnscaled / rescaleSize) == rightUnscaled;
        }

        // at least one side is decimal16, compare as BigDecimal
        BigDecimal leftDecimal = decodeAsBigDecimal(leftSlice, leftOffset, leftType);
        BigDecimal rightDecimal = decodeAsBigDecimal(rightSlice, rightOffset, rightType);
        return leftDecimal.compareTo(rightDecimal) == 0;
    }

    private static BigDecimal decodeAsBigDecimal(Slice leftSlice, int leftOffset, PrimitiveType leftType)
    {
        return switch (leftType) {
            case INT8 -> BigDecimal.valueOf(leftSlice.getByte(leftOffset + 1));
            case INT16 -> BigDecimal.valueOf(leftSlice.getShort(leftOffset + 1));
            case INT32 -> BigDecimal.valueOf(leftSlice.getInt(leftOffset + 1));
            case INT64 -> BigDecimal.valueOf(leftSlice.getLong(leftOffset + 1));
            case DECIMAL4 -> {
                int scale = leftSlice.getByte(leftOffset + 1);
                int unscaled = leftSlice.getInt(leftOffset + 2);
                yield BigDecimal.valueOf(unscaled, scale);
            }
            case DECIMAL8 -> {
                int scale = leftSlice.getByte(leftOffset + 1);
                long unscaled = leftSlice.getLong(leftOffset + 2);
                yield BigDecimal.valueOf(unscaled, scale);
            }
            case DECIMAL16 -> {
                int scale = leftSlice.getByte(leftOffset + 1);
                long low = leftSlice.getLong(leftOffset + 2);
                long high = leftSlice.getLong(leftOffset + 10);
                BigInteger unscaled = toBigInteger(high, low);
                yield new BigDecimal(unscaled, scale);
            }
            default -> throw new IllegalStateException("Unexpected left type: " + leftType);
        };
    }

    private static BigInteger toBigInteger(long high, long low)
    {
        byte[] bytes = new byte[16];
        BIG_ENDIAN_LONG_VIEW.set(bytes, 0, high);
        BIG_ENDIAN_LONG_VIEW.set(bytes, Long.BYTES, low);
        return new BigInteger(bytes);
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
            EquivalenceClass equivalenceClass = getEquivalenceClass(header);
            addInt(equivalenceClass.ordinal());
            switch (equivalenceClass) {
                case NULL -> addLong(0);
                case BOOLEAN -> addLong(getPrimitiveType(header) == PrimitiveType.BOOLEAN_TRUE ? 1 : 2);
                case EXACT_NUMERIC -> hashExactNumeric(getPrimitiveType(header), slice, offset);
                case FLOAT -> {
                    float value = slice.getFloat(offset + 1);
                    if (value == 0.0f) {
                        // normalize -0.0f and 0.0f to the same hash
                        value = 0.0f;
                    }
                    addFloat(value);
                }
                case DOUBLE -> {
                    double value = slice.getDouble(offset + 1);
                    if (value == 0.0) {
                        // normalize -0.0 and 0.0 to the same hash
                        value = 0.0;
                    }
                    addDouble(value);
                }
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

        private void addFloat(float value)
        {
            addLong(Float.floatToIntBits(value));
        }

        private void hashExactNumeric(PrimitiveType primitiveType, Slice slice, int offset)
        {
            switch (primitiveType) {
                case INT8 -> addLong(slice.getByte(offset + 1));
                case INT16 -> addLong(slice.getShort(offset + 1));
                case INT32 -> addLong(slice.getInt(offset + 1));
                case INT64 -> addLong(slice.getLong(offset + 1));
                case DECIMAL4, DECIMAL8 -> {
                    int scale = slice.getByte(offset + 1);
                    long unscaled = primitiveType == DECIMAL4 ? slice.getInt(offset + 2) : slice.getLong(offset + 2);
                    if (unscaled == 0) {
                        addLong(0);
                        return;
                    }
                    if (scale >= 0) {
                        while (scale > 0 && (unscaled % 10L) == 0L) {
                            unscaled /= 10L;
                            scale--;
                        }
                    }
                    // Do not write a zero scale, so that 123.0 and 123 are treated the same
                    if (scale != 0) {
                        addInt(scale);
                    }
                    addLong(unscaled);
                }
                case DECIMAL16 -> {
                    int scale = slice.getByte(offset + 1);
                    long low = slice.getLong(offset + 2);
                    long high = slice.getLong(offset + 10);

                    // strip trailing zeros in decimal part
                    if (scale != 0) {
                        // use BigDecimal to strip trailing zeros
                        BigDecimal decimal = new BigDecimal(toBigInteger(high, low), scale);
                        decimal = decimal.stripTrailingZeros();
                        if (decimal.scale() < 0) {
                            decimal = decimal.setScale(0, RoundingMode.UNNECESSARY);
                        }
                        BigInteger bigInteger = decimal.unscaledValue();
                        scale = decimal.scale();
                        low = bigInteger.longValue();
                        high = bigInteger.shiftRight(64).longValue();
                    }
                    // does the unscaled value fit in a long?
                    if (high == 0L || high == -1L && low < 0L) {
                        // Do not write a zero scale, so that 123.0 and 123 are treated the same
                        if (scale != 0) {
                            addInt(scale);
                        }
                        addLong(low);
                        return;
                    }

                    // decimal does not fit in a long, so it can only match another decimal that also does not fit in a long
                    // mix high and low parts and scale into hash
                    addInt(scale);
                    addLong(high);
                    addLong(low);
                }
                default -> throw new IllegalStateException("Unexpected exact numeric type: " + primitiveType);
            }
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
