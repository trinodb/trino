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
import io.trino.spi.type.Int128;
import io.trino.spi.type.Int128Math;

import java.lang.runtime.ExactConversionsSupport;

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
import static io.trino.spi.variant.VariantUtils.readOffset;

final class VariantEquality
{
    private VariantEquality() {}

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

        if (VariantUtils.isExactNumeric(leftType) && VariantUtils.isExactNumeric(rightType)) {
            return equalsExactNumeric(leftHeader, leftSlice, leftOffset, rightHeader, rightSlice, rightOffset);
        }

        if (VariantUtils.isFloatingNumeric(leftType) && VariantUtils.isFloatingNumeric(rightType)) {
            return equalsFloatingNumeric(leftType, leftSlice, leftOffset, rightType, rightSlice, rightOffset);
        }

        if (VariantUtils.isExactNumeric(leftType)) {
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

        if ((leftType == INT8 || leftType == INT16 || leftType == INT32 || leftType == INT64) &&
                (rightType == INT8 || rightType == INT16 || rightType == INT32 || rightType == INT64)) {
            long leftValue = switch (leftType) {
                case INT8 -> leftSlice.getByte(leftOffset + 1);
                case INT16 -> leftSlice.getShort(leftOffset + 1);
                case INT32 -> leftSlice.getInt(leftOffset + 1);
                case INT64 -> leftSlice.getLong(leftOffset + 1);
                default -> throw new VerifyException("Unexpected integer type: " + leftType);
            };
            long rightValue = switch (rightType) {
                case INT8 -> rightSlice.getByte(rightOffset + 1);
                case INT16 -> rightSlice.getShort(rightOffset + 1);
                case INT32 -> rightSlice.getInt(rightOffset + 1);
                case INT64 -> rightSlice.getLong(rightOffset + 1);
                default -> throw new VerifyException("Unexpected integer type: " + rightType);
            };
            return leftValue == rightValue;
        }
        return VariantUtils.decodeExactNumericCanonical(leftType, leftSlice, leftOffset)
                .equals(VariantUtils.decodeExactNumericCanonical(rightType, rightSlice, rightOffset));
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
        double leftValue = VariantUtils.floatingAsDouble(leftType, leftSlice, leftOffset);
        double rightValue = VariantUtils.floatingAsDouble(rightType, rightSlice, rightOffset);

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
        double floatingValue = VariantUtils.floatingAsDouble(floatingType, floatingSlice, floatingOffset);
        if (!Double.isFinite(floatingValue)) {
            return false;
        }

        if (floatingValue == 0.0 || ExactConversionsSupport.isDoubleToLongExact(floatingValue)) {
            return equalsExactNumericWithLong(exactType, exactSlice, exactOffset, (long) floatingValue);
        }

        VariantUtils.Decimal128Canonical floatingDecimal = VariantUtils.tryToDecimal128Exact(floatingValue);
        if (floatingDecimal == null) {
            return false;
        }

        VariantUtils.Decimal128Canonical exactDecimal = VariantUtils.decodeExactNumericCanonical(exactType, exactSlice, exactOffset);
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
            default -> throw new VerifyException("Unexpected exact numeric type: " + exactType);
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
            return false;
        }
        try {
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
            Int128Math.multiply(value >> 63, value, powerOfTen.getHigh(), powerOfTen.getLow(), scaledValue, 0);
        }
        catch (ArithmeticException ignored) {
            return false;
        }
        return unscaled.getHigh() == scaledValue[0] && unscaled.getLow() == scaledValue[1];
    }

    private static boolean equalsTimestamp(
            byte leftHeader,
            Slice leftSlice,
            int leftOffset,
            byte rightHeader,
            Slice rightSlice,
            int rightOffset)
    {
        long leftValue = leftSlice.getLong(leftOffset + 1);
        long rightValue = rightSlice.getLong(rightOffset + 1);

        if (leftHeader == rightHeader) {
            return leftValue == rightValue;
        }

        PrimitiveType leftType = getPrimitiveType(leftHeader);
        if (leftType == PrimitiveType.TIMESTAMP_UTC_MICROS || leftType == PrimitiveType.TIMESTAMP_NTZ_MICROS) {
            return rightValue % 1_000L == 0 && leftValue == (rightValue / 1_000L);
        }
        return leftValue % 1_000L == 0 && (leftValue / 1_000L) == rightValue;
    }

    private static boolean equalsStringLike(
            Slice leftSlice,
            int leftOffset,
            byte leftHeader,
            Slice rightSlice,
            int rightOffset,
            byte rightHeader)
    {
        int leftStringOffset;
        int leftStringLength;
        if (getBasicType(leftHeader) == SHORT_STRING) {
            leftStringLength = shortStringLength(leftHeader);
            leftStringOffset = leftOffset + 1;
        }
        else {
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
            Metadata leftMetadata,
            Slice leftSlice,
            int leftOffset,
            Metadata rightMetadata,
            Slice rightSlice,
            int rightOffset)
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
            Metadata leftMetadata,
            Slice leftSlice,
            int leftOffset,
            Metadata rightMetadata,
            Slice rightSlice,
            int rightOffset)
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

    private enum ValueClass
    {
        NULL,
        BOOLEAN,
        NUMERIC,
        DATE,
        TIME_NTZ,
        TIMESTAMP_UTC,
        TIMESTAMP_NTZ,
        BINARY,
        STRING,
        UUID,
        OBJECT,
        ARRAY;

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
}
