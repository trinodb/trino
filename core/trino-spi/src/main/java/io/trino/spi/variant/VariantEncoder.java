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
import io.trino.spi.variant.Header.PrimitiveType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.function.IntFunction;
import java.util.function.IntUnaryOperator;

import static io.trino.spi.variant.Header.PrimitiveType.BINARY;
import static io.trino.spi.variant.Header.PrimitiveType.BOOLEAN_FALSE;
import static io.trino.spi.variant.Header.PrimitiveType.BOOLEAN_TRUE;
import static io.trino.spi.variant.Header.PrimitiveType.DATE;
import static io.trino.spi.variant.Header.PrimitiveType.DECIMAL16;
import static io.trino.spi.variant.Header.PrimitiveType.DECIMAL4;
import static io.trino.spi.variant.Header.PrimitiveType.DECIMAL8;
import static io.trino.spi.variant.Header.PrimitiveType.DOUBLE;
import static io.trino.spi.variant.Header.PrimitiveType.FLOAT;
import static io.trino.spi.variant.Header.PrimitiveType.INT16;
import static io.trino.spi.variant.Header.PrimitiveType.INT32;
import static io.trino.spi.variant.Header.PrimitiveType.INT64;
import static io.trino.spi.variant.Header.PrimitiveType.INT8;
import static io.trino.spi.variant.Header.PrimitiveType.NULL;
import static io.trino.spi.variant.Header.PrimitiveType.STRING;
import static io.trino.spi.variant.Header.PrimitiveType.TIMESTAMP_NTZ_MICROS;
import static io.trino.spi.variant.Header.PrimitiveType.TIMESTAMP_NTZ_NANOS;
import static io.trino.spi.variant.Header.PrimitiveType.TIMESTAMP_UTC_MICROS;
import static io.trino.spi.variant.Header.PrimitiveType.TIMESTAMP_UTC_NANOS;
import static io.trino.spi.variant.Header.PrimitiveType.TIME_NTZ_MICROS;
import static io.trino.spi.variant.Header.SHORT_STRING_MAX_LENGTH;
import static io.trino.spi.variant.Header.arrayHeader;
import static io.trino.spi.variant.Header.objectHeader;
import static io.trino.spi.variant.Header.primitiveHeader;
import static io.trino.spi.variant.Header.shortStringHeader;
import static io.trino.spi.variant.VariantUtils.checkArgument;
import static io.trino.spi.variant.VariantUtils.getOffsetSize;
import static io.trino.spi.variant.VariantUtils.verify;
import static io.trino.spi.variant.VariantUtils.writeOffset;
import static java.lang.Math.max;

public final class VariantEncoder
{
    public static final int ENCODED_NULL_SIZE = 1;
    public static final int ENCODED_BOOLEAN_SIZE = 1;
    public static final int ENCODED_BYTE_SIZE = 2;
    public static final int ENCODED_SHORT_SIZE = 3;
    public static final int ENCODED_INT_SIZE = 5;
    public static final int ENCODED_LONG_SIZE = 9;
    public static final int ENCODED_DECIMAL4_SIZE = 6;
    public static final int ENCODED_DECIMAL8_SIZE = 10;
    public static final int ENCODED_DECIMAL16_SIZE = 18;
    public static final int ENCODED_FLOAT_SIZE = 5;
    public static final int ENCODED_DOUBLE_SIZE = 9;
    public static final int ENCODED_DATE_SIZE = 5;
    public static final int ENCODED_TIME_SIZE = 9;
    public static final int ENCODED_TIMESTAMP_SIZE = 9;
    public static final int ENCODED_UUID_SIZE = 17;
    public static final int ENCODED_EMPTY_OBJECT_SIZE = 3;

    private VariantEncoder() {}

    public static int encodeNull(Slice variant, int offset)
    {
        variant.setByte(offset, primitiveHeader(NULL));
        return ENCODED_NULL_SIZE;
    }

    public static int encodeBoolean(boolean value, Slice variant, int offset)
    {
        if (value) {
            variant.setByte(offset, primitiveHeader(BOOLEAN_TRUE));
        }
        else {
            variant.setByte(offset, primitiveHeader(BOOLEAN_FALSE));
        }
        return ENCODED_BOOLEAN_SIZE;
    }

    public static int encodeByte(byte value, Slice variant, int offset)
    {
        variant.setByte(offset, primitiveHeader(INT8));
        variant.setByte(offset + 1, value);
        return ENCODED_BYTE_SIZE;
    }

    public static int encodeShort(short value, Slice variant, int offset)
    {
        variant.setByte(offset, primitiveHeader(INT16));
        variant.setShort(offset + 1, value);
        return ENCODED_SHORT_SIZE;
    }

    public static int encodeInt(int value, Slice variant, int offset)
    {
        variant.setByte(offset, primitiveHeader(INT32));
        variant.setInt(offset + 1, value);
        return ENCODED_INT_SIZE;
    }

    public static int encodeLong(long value, Slice variant, int offset)
    {
        variant.setByte(offset, primitiveHeader(INT64));
        variant.setLong(offset + 1, value);
        return ENCODED_LONG_SIZE;
    }

    public static int encodedDecimalSize(BigDecimal value)
    {
        int precision = value.precision();
        if (precision <= 9) {
            return ENCODED_DECIMAL4_SIZE;
        }
        else if (precision <= 18) {
            return ENCODED_DECIMAL8_SIZE;
        }
        else if (precision <= 38) {
            return ENCODED_DECIMAL16_SIZE;
        }
        throw new IllegalArgumentException("Decimal precision out of range: " + precision);
    }

    public static int encodeDecimal(BigDecimal value, Slice data, int offset)
    {
        // We could improve the fit into int/long by checking the actual unscaled value,
        // but this code matches the existing Iceberg logic.
        int precision = value.precision();
        BigInteger unscaled = value.unscaledValue();
        int scale = value.scale();
        if (precision <= 9) {
            return encodeDecimal4(unscaled.intValue(), scale, data, offset);
        }
        else if (precision <= 18) {
            return encodeDecimal8(unscaled.longValue(), scale, data, offset);
        }
        else if (precision <= 38) {
            return encodeDecimal16(unscaled, scale, data, offset);
        }
        throw new IllegalArgumentException("Decimal precision out of range: " + precision);
    }

    public static int encodeDecimal4(int unscaled, int scale, Slice data, int offset)
    {
        validateDecimalScale(scale);
        data.setByte(offset, primitiveHeader(DECIMAL4));
        data.setByte(offset + 1, (byte) scale);
        data.setInt(offset + 2, unscaled);
        return ENCODED_DECIMAL4_SIZE;
    }

    public static int encodeDecimal8(long unscaled, int scale, Slice data, int offset)
    {
        validateDecimalScale(scale);
        data.setByte(offset, primitiveHeader(DECIMAL8));
        data.setByte(offset + 1, (byte) scale);
        data.setLong(offset + 2, unscaled);
        return ENCODED_DECIMAL8_SIZE;
    }

    public static int encodeDecimal16(BigInteger unscaled, int scale, Slice data, int offset)
    {
        long low = unscaled.longValue();
        long high;
        try {
            high = unscaled.shiftRight(64).longValueExact();
        }
        catch (ArithmeticException e) {
            throw new ArithmeticException("BigInteger out of Int128 range");
        }

        return encodeDecimal16(high, low, scale, data, offset);
    }

    public static int encodeDecimal16(Int128 unscaled, int scale, Slice data, int offset)
    {
        return encodeDecimal16(unscaled.getHigh(), unscaled.getLow(), scale, data, offset);
    }

    public static int encodeDecimal16(long high, long low, int scale, Slice data, int offset)
    {
        validateDecimalScale(scale);

        data.setByte(offset, primitiveHeader(DECIMAL16));
        data.setByte(offset + 1, (byte) scale);

        // int128 little-endian: low 64 bits first, then high 64 bits
        data.setLong(offset + 2, low);
        data.setLong(offset + 10, high);

        return ENCODED_DECIMAL16_SIZE;
    }

    private static void validateDecimalScale(int scale)
    {
        checkArgument(scale >= 0 && scale <= 38, () -> "Invalid decimal scale: %s (expected 0..38)".formatted(scale));
    }

    public static int encodeFloat(float value, Slice variant, int offset)
    {
        variant.setByte(offset, primitiveHeader(FLOAT));
        variant.setFloat(offset + 1, value);
        return ENCODED_FLOAT_SIZE;
    }

    public static int encodeDouble(double value, Slice variant, int offset)
    {
        variant.setByte(offset, primitiveHeader(DOUBLE));
        variant.setDouble(offset + 1, value);
        return ENCODED_DOUBLE_SIZE;
    }

    public static int encodeDate(int value, Slice variant, int offset)
    {
        variant.setByte(offset, primitiveHeader(DATE));
        variant.setInt(offset + 1, value);
        return ENCODED_DATE_SIZE;
    }

    public static int encodeDate(LocalDate value, Slice variant, int offset)
    {
        return encodeDate((int) value.toEpochDay(), variant, offset);
    }

    public static int encodeTimeMicrosNtz(long value, Slice variant, int offset)
    {
        variant.setByte(offset, primitiveHeader(TIME_NTZ_MICROS));
        variant.setLong(offset + 1, value);
        return ENCODED_TIME_SIZE;
    }

    public static int encodeTimeMicrosNtz(LocalTime value, Slice variant, int offset)
    {
        long nanoOfDay = value.toNanoOfDay();
        long microsOfDay = nanoOfDay / 1_000;
        return encodeTimeMicrosNtz(microsOfDay, variant, offset);
    }

    public static int encodeTimestampMicrosUtc(long value, Slice variant, int offset)
    {
        variant.setByte(offset, primitiveHeader(TIMESTAMP_UTC_MICROS));
        variant.setLong(offset + 1, value);
        return ENCODED_TIMESTAMP_SIZE;
    }

    public static int encodeTimestampMicrosUtc(Instant value, Slice variant, int offset)
    {
        long epochSecond = value.getEpochSecond();
        int nanoOfSecond = value.getNano();
        long epochMicros = epochSecond * 1_000_000 + (nanoOfSecond / 1_000);
        return encodeTimestampMicrosUtc(epochMicros, variant, offset);
    }

    public static int encodeTimestampNanosUtc(long value, Slice variant, int offset)
    {
        variant.setByte(offset, primitiveHeader(TIMESTAMP_UTC_NANOS));
        variant.setLong(offset + 1, value);
        return ENCODED_TIMESTAMP_SIZE;
    }

    public static int encodeTimestampNanosUtc(Instant value, Slice variant, int offset)
    {
        long epochSecond = value.getEpochSecond();
        int nanoOfSecond = value.getNano();
        long epochNanos = epochSecond * 1_000_000_000 + nanoOfSecond;
        return encodeTimestampNanosUtc(epochNanos, variant, offset);
    }

    public static int encodeTimestampMicrosNtz(long value, Slice variant, int offset)
    {
        variant.setByte(offset, primitiveHeader(TIMESTAMP_NTZ_MICROS));
        variant.setLong(offset + 1, value);
        return ENCODED_TIMESTAMP_SIZE;
    }

    public static int encodeTimestampMicrosNtz(LocalDateTime value, Slice variant, int offset)
    {
        long seconds = value.toEpochSecond(ZoneOffset.UTC);
        int nanoOfSecond = value.getNano();
        long micros = seconds * 1_000_000 + (nanoOfSecond / 1_000);
        return encodeTimestampMicrosNtz(micros, variant, offset);
    }

    public static int encodeTimestampNanosNtz(long value, Slice variant, int offset)
    {
        variant.setByte(offset, primitiveHeader(TIMESTAMP_NTZ_NANOS));
        variant.setLong(offset + 1, value);
        return ENCODED_TIMESTAMP_SIZE;
    }

    public static int encodeTimestampNanosNtz(LocalDateTime value, Slice variant, int offset)
    {
        long seconds = value.toEpochSecond(ZoneOffset.UTC);
        int nanoOfSecond = value.getNano();
        long nanos = seconds * 1_000_000_000 + nanoOfSecond;
        return encodeTimestampNanosNtz(nanos, variant, offset);
    }

    public static int encodedBinarySize(int length)
    {
        return 5 + length;
    }

    public static int encodeBinary(Slice value, Slice variant, int offset)
    {
        variant.setByte(offset, primitiveHeader(BINARY));
        variant.setInt(offset + 1, value.length());
        variant.setBytes(offset + 5, value);
        return encodedBinarySize(value.length());
    }

    public static int encodedStringSize(int length)
    {
        if (length <= SHORT_STRING_MAX_LENGTH) {
            return 1 + length;
        }
        return 5 + length;
    }

    public static int encodeString(Slice value, Slice variant, int offset)
    {
        if (value.length() <= SHORT_STRING_MAX_LENGTH) {
            variant.setByte(offset, shortStringHeader(value.length()));
            variant.setBytes(offset + 1, value);
            return 1 + value.length();
        }
        variant.setByte(offset, primitiveHeader(STRING));
        variant.setInt(offset + 1, value.length());
        variant.setBytes(offset + 5, value);
        return 5 + value.length();
    }

    public static int encodeUuid(Slice value, Slice variant, int offset)
    {
        checkArgument(value.length() == 16, "UUID slice must be 16 bytes long");
        variant.setByte(offset, primitiveHeader(PrimitiveType.UUID));
        variant.setBytes(offset + 1, value);
        return ENCODED_UUID_SIZE;
    }

    public static int encodeUuid(UUID value, Slice variant, int offset)
    {
        variant.setByte(offset, primitiveHeader(PrimitiveType.UUID));
        variant.setLong(offset + 1, Long.reverseBytes(value.getMostSignificantBits()));
        variant.setLong(offset + 9, Long.reverseBytes(value.getLeastSignificantBits()));
        return ENCODED_UUID_SIZE;
    }

    public static int encodedArraySize(int elementCount, int totalElementsLength)
    {
        boolean large = elementCount > 255;

        int offsetSize = getOffsetSize(totalElementsLength);
        int offsetsLength = (elementCount + 1) * offsetSize;

        return 1 + (large ? 4 : 1) + offsetsLength + totalElementsLength;
    }

    public static int encodeArrayHeading(int elementCount, IntUnaryOperator elementLength, Slice variant, int offset)
    {
        boolean large = elementCount > 255;

        int totalElementsLength = 0;
        for (int i = 0; i < elementCount; i++) {
            totalElementsLength += elementLength.applyAsInt(i);
        }
        int offsetSize = getOffsetSize(totalElementsLength);
        int offsetsLength = (elementCount + 1) * offsetSize;

        int headerSize = 1 + (large ? 4 : 1) + offsetsLength;
        int expectedVariantSize = headerSize + totalElementsLength;
        checkArgument(variant.length() >= offset + expectedVariantSize, () -> "Variant slice is too small to encode array of size " + expectedVariantSize);

        int position = offset;
        // write header
        variant.setByte(position, arrayHeader(offsetSize, large));
        position += 1;

        // write element count
        if (large) {
            variant.setInt(offset + 1, elementCount);
            position += 4;
        }
        else {
            variant.setByte(offset + 1, (byte) elementCount);
            position += 1;
        }

        // write offsets
        int dataOffset = 0;
        writeOffset(variant, position, dataOffset, offsetSize);
        position += offsetSize;
        for (int i = 0; i < elementCount; i++) {
            dataOffset += elementLength.applyAsInt(i);
            writeOffset(variant, position, dataOffset, offsetSize);
            position += offsetSize;
        }
        verify(position == offset + headerSize, "Encoded size does not match expected size");
        return headerSize;
    }

    public static int encodeArray(List<Slice> elements, Slice variant, int offset)
    {
        int written = encodeArrayHeading(elements.size(), index -> elements.get(index).length(), variant, offset);

        // write elements
        for (Slice element : elements) {
            variant.setBytes(offset + written, element);
            written += element.length();
        }
        return written;
    }

    public static int encodedObjectSize(int maxField, int elementCount, int totalElementsLength)
    {
        boolean large = elementCount > 255;

        int fieldIdSize = getOffsetSize(maxField);
        int fieldIdsLength = elementCount * fieldIdSize;
        int offsetSize = getOffsetSize(totalElementsLength);
        int offsetsLength = (elementCount + 1) * offsetSize;

        return 1 + (large ? 4 : 1) + fieldIdsLength + offsetsLength + totalElementsLength;
    }

    /// Encodes an object with the given field count, field IDs, and field values into the provided variant slice at the specified offset.
    /// The field IDs and field values are provided as functions that take an index and return the corresponding value. The field IDs must
    /// be returned in sorted order, or the resulting encoding will be invalid.
    public static int encodeObject(int fieldCount, IntUnaryOperator fieldIds, IntFunction<Slice> fieldValue, Slice variant, int offset)
    {
        int written = encodeObjectHeading(
                fieldCount,
                fieldIds,
                index -> fieldValue.apply(index).length(),
                variant,
                offset);
        for (int i = 0; i < fieldCount; i++) {
            Slice data = fieldValue.apply(i);
            variant.setBytes(offset + written, data);
            written += data.length();
        }
        return written;
    }

    /// Encodes the heading of an object with the given field count, field IDs, and field lengths into the provided variant slice at the specified offset.
    /// The field IDs and field lengths are provided as functions that take an index and return the corresponding value. The field IDs must
    /// be returned in sorted order, or the resulting encoding will be invalid.
    public static int encodeObjectHeading(int fieldCount, IntUnaryOperator fieldIds, IntUnaryOperator fieldLength, Slice variant, int offset)
    {
        if (fieldCount == 0) {
            checkArgument(variant.length() >= offset + ENCODED_EMPTY_OBJECT_SIZE, "Variant slice is too small to encode empty object");
            variant.setByte(offset, objectHeader(1, 1, false));
            variant.setByte(offset + 1, 0); // zero elements
            variant.setByte(offset + 2, 0); // first offest is zero (required)
            return ENCODED_EMPTY_OBJECT_SIZE;
        }

        boolean large = fieldCount > 255;

        int maxFieldId = -1;
        int totalElementsLength = 0;
        for (int i = 0; i < fieldCount; i++) {
            maxFieldId = max(maxFieldId, fieldIds.applyAsInt(i));
            totalElementsLength += fieldLength.applyAsInt(i);
        }

        int fieldIdSize = getOffsetSize(maxFieldId);
        int fieldIdsLength = fieldCount * fieldIdSize;
        int offsetSize = getOffsetSize(totalElementsLength);
        int offsetsLength = (fieldCount + 1) * offsetSize;

        int headerSize = 1 + (large ? 4 : 1) + fieldIdsLength + offsetsLength;
        int expectedVariantSize = headerSize + totalElementsLength;
        checkArgument(variant.length() >= offset + expectedVariantSize, () -> "Variant slice is too small to encode object of size " + expectedVariantSize);

        int position = offset;
        // write header
        variant.setByte(position, objectHeader(fieldIdSize, offsetSize, large));
        position += 1;

        // write element count
        if (large) {
            variant.setInt(position, fieldCount);
            position += 4;
        }
        else {
            variant.setByte(position, (byte) fieldCount);
            position += 1;
        }

        // write field IDs
        for (int i = 0; i < fieldCount; i++) {
            writeOffset(variant, position, fieldIds.applyAsInt(i), fieldIdSize);
            position += fieldIdSize;
        }

        // write offsets
        int dataOffset = 0;
        writeOffset(variant, position, dataOffset, offsetSize);
        position += offsetSize;
        for (int i = 0; i < fieldCount; i++) {
            dataOffset += fieldLength.applyAsInt(i);
            writeOffset(variant, position, dataOffset, offsetSize);
            position += offsetSize;
        }

        verify(position == offset + headerSize, "Encoded size does not match expected size");
        return headerSize;
    }
}
