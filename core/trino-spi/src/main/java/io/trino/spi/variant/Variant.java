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
import io.airlift.slice.Slices;
import io.trino.spi.variant.Header.BasicType;
import io.trino.spi.variant.Header.PrimitiveType;
import io.trino.spi.variant.Metadata.Builder.SortedMetadata;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.IntUnaryOperator;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.variant.Header.BasicType.OBJECT;
import static io.trino.spi.variant.Header.BasicType.PRIMITIVE;
import static io.trino.spi.variant.Header.arrayFieldOffsetSize;
import static io.trino.spi.variant.Header.arrayIsLarge;
import static io.trino.spi.variant.Header.getBasicType;
import static io.trino.spi.variant.Header.getPrimitiveType;
import static io.trino.spi.variant.Header.objectFieldIdSize;
import static io.trino.spi.variant.Header.objectFieldOffsetSize;
import static io.trino.spi.variant.Header.objectIsLarge;
import static io.trino.spi.variant.Header.primitiveHeader;
import static io.trino.spi.variant.Header.shortStringLength;
import static io.trino.spi.variant.Metadata.EMPTY_METADATA;
import static io.trino.spi.variant.VariantEncoder.ENCODED_BOOLEAN_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_BYTE_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_DATE_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_DOUBLE_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_EMPTY_OBJECT_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_FLOAT_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_INT_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_LONG_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_NULL_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_SHORT_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_TIMESTAMP_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_TIME_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_UUID_SIZE;
import static io.trino.spi.variant.VariantEncoder.encodeArrayHeading;
import static io.trino.spi.variant.VariantEncoder.encodeBinary;
import static io.trino.spi.variant.VariantEncoder.encodeBoolean;
import static io.trino.spi.variant.VariantEncoder.encodeByte;
import static io.trino.spi.variant.VariantEncoder.encodeDate;
import static io.trino.spi.variant.VariantEncoder.encodeDecimal;
import static io.trino.spi.variant.VariantEncoder.encodeDouble;
import static io.trino.spi.variant.VariantEncoder.encodeFloat;
import static io.trino.spi.variant.VariantEncoder.encodeInt;
import static io.trino.spi.variant.VariantEncoder.encodeLong;
import static io.trino.spi.variant.VariantEncoder.encodeNull;
import static io.trino.spi.variant.VariantEncoder.encodeObjectHeading;
import static io.trino.spi.variant.VariantEncoder.encodeShort;
import static io.trino.spi.variant.VariantEncoder.encodeString;
import static io.trino.spi.variant.VariantEncoder.encodeTimeMicrosNtz;
import static io.trino.spi.variant.VariantEncoder.encodeTimestampMicrosNtz;
import static io.trino.spi.variant.VariantEncoder.encodeTimestampMicrosUtc;
import static io.trino.spi.variant.VariantEncoder.encodeTimestampNanosNtz;
import static io.trino.spi.variant.VariantEncoder.encodeTimestampNanosUtc;
import static io.trino.spi.variant.VariantEncoder.encodeUuid;
import static io.trino.spi.variant.VariantEncoder.encodedArraySize;
import static io.trino.spi.variant.VariantEncoder.encodedBinarySize;
import static io.trino.spi.variant.VariantEncoder.encodedDecimalSize;
import static io.trino.spi.variant.VariantEncoder.encodedObjectSize;
import static io.trino.spi.variant.VariantEncoder.encodedStringSize;
import static io.trino.spi.variant.VariantUtils.checkArgument;
import static io.trino.spi.variant.VariantUtils.checkState;
import static io.trino.spi.variant.VariantUtils.findFieldIndex;
import static io.trino.spi.variant.VariantUtils.readOffset;
import static io.trino.spi.variant.VariantUtils.verify;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.toIntExact;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

public final class Variant
{
    public static final Variant NULL_VALUE = from(EMPTY_METADATA, Slices.wrappedBuffer(primitiveHeader(PrimitiveType.NULL)));
    public static final Variant EMPTY_ARRAY;
    public static final Variant EMPTY_OBJECT;
    private final Slice data;
    private final Metadata metadata;
    private final BasicType basicType;
    private final PrimitiveType primitiveType;

    static {
        IntUnaryOperator emptyIndexedOperator = index -> {
            throw new IndexOutOfBoundsException();
        };

        Slice emptyArrayValue = Slices.allocate(encodedArraySize(0, 0));
        encodeArrayHeading(0, emptyIndexedOperator, emptyArrayValue, 0);
        EMPTY_ARRAY = from(EMPTY_METADATA, emptyArrayValue);

        Slice emptyObjectValue = Slices.allocate(encodedObjectSize(0, 0, 0));
        encodeObjectHeading(0, emptyIndexedOperator, emptyIndexedOperator, emptyObjectValue, 0);
        EMPTY_OBJECT = from(EMPTY_METADATA, emptyObjectValue);
    }

    public Variant(Slice data, Metadata metadata, BasicType basicType, PrimitiveType primitiveType)
    {
        requireNonNull(data, "data is null");
        requireNonNull(metadata, "metadata is null");
        requireNonNull(basicType, "basicType is null");
        checkArgument(basicType == PRIMITIVE || primitiveType == null, "primitiveType must be null for non-primitive basicType");
        checkArgument(basicType != PRIMITIVE || primitiveType != null, "primitiveType must be non-null for primitive basicType");

        // not need to retain metadata for non-container types
        if (!basicType.isContainer()) {
            metadata = EMPTY_METADATA;
        }
        this.data = data;
        this.metadata = metadata;
        this.basicType = basicType;
        this.primitiveType = primitiveType;
    }

    public Slice data()
    {
        return data;
    }

    public Metadata metadata()
    {
        return metadata;
    }

    public BasicType basicType()
    {
        return basicType;
    }

    public PrimitiveType primitiveType()
    {
        return primitiveType;
    }

    public static Variant from(Metadata metadata, Slice data)
    {
        byte header = data.getByte(0);
        BasicType basicType = getBasicType(header);
        PrimitiveType primitiveType = basicType == PRIMITIVE ? getPrimitiveType(header) : null;
        return new Variant(data, metadata, basicType, primitiveType);
    }

    public static Variant ofBoolean(boolean value)
    {
        Slice data = Slices.allocate(ENCODED_BOOLEAN_SIZE);
        encodeBoolean(value, data, 0);
        return from(EMPTY_METADATA, data);
    }

    public static Variant ofByte(byte value)
    {
        Slice data = Slices.allocate(ENCODED_BYTE_SIZE);
        encodeByte(value, data, 0);
        return from(EMPTY_METADATA, data);
    }

    public static Variant ofShort(short value)
    {
        Slice data = Slices.allocate(ENCODED_SHORT_SIZE);
        encodeShort(value, data, 0);
        return from(EMPTY_METADATA, data);
    }

    public static Variant ofInt(int value)
    {
        Slice data = Slices.allocate(ENCODED_INT_SIZE);
        encodeInt(value, data, 0);
        return from(EMPTY_METADATA, data);
    }

    public static Variant ofLong(long value)
    {
        Slice data = Slices.allocate(ENCODED_LONG_SIZE);
        encodeLong(value, data, 0);
        return from(EMPTY_METADATA, data);
    }

    public static Variant ofDecimal(BigDecimal value)
    {
        requireNonNull(value, "value is null");
        Slice data = Slices.allocate(encodedDecimalSize(value));
        encodeDecimal(value, data, 0);
        return from(EMPTY_METADATA, data);
    }

    public static Variant ofFloat(float value)
    {
        Slice data = Slices.allocate(ENCODED_FLOAT_SIZE);
        encodeFloat(value, data, 0);
        return from(EMPTY_METADATA, data);
    }

    public static Variant ofDouble(double value)
    {
        Slice data = Slices.allocate(ENCODED_DOUBLE_SIZE);
        encodeDouble(value, data, 0);
        return from(EMPTY_METADATA, data);
    }

    public static Variant ofDate(int value)
    {
        Slice data = Slices.allocate(ENCODED_DATE_SIZE);
        encodeDate(value, data, 0);
        return from(EMPTY_METADATA, data);
    }

    public static Variant ofDate(LocalDate value)
    {
        Slice data = Slices.allocate(ENCODED_DATE_SIZE);
        encodeDate(value, data, 0);
        return from(EMPTY_METADATA, data);
    }

    public static Variant ofTimeMicrosNtz(long value)
    {
        Slice data = Slices.allocate(ENCODED_TIME_SIZE);
        encodeTimeMicrosNtz(value, data, 0);
        return from(EMPTY_METADATA, data);
    }

    public static Variant ofTimeMicrosNtz(LocalTime value)
    {
        Slice data = Slices.allocate(ENCODED_TIME_SIZE);
        encodeTimeMicrosNtz(value, data, 0);
        return from(EMPTY_METADATA, data);
    }

    public static Variant ofTimestampMicrosUtc(long value)
    {
        Slice data = Slices.allocate(ENCODED_TIMESTAMP_SIZE);
        encodeTimestampMicrosUtc(value, data, 0);
        return from(EMPTY_METADATA, data);
    }

    public static Variant ofTimestampMicrosUtc(Instant value)
    {
        Slice data = Slices.allocate(ENCODED_TIMESTAMP_SIZE);
        encodeTimestampMicrosUtc(value, data, 0);
        return from(EMPTY_METADATA, data);
    }

    public static Variant ofTimestampNanosUtc(long value)
    {
        Slice data = Slices.allocate(ENCODED_TIMESTAMP_SIZE);
        encodeTimestampNanosUtc(value, data, 0);
        return from(EMPTY_METADATA, data);
    }

    public static Variant ofTimestampNanosUtc(Instant value)
    {
        Slice data = Slices.allocate(ENCODED_TIMESTAMP_SIZE);
        encodeTimestampNanosUtc(value, data, 0);
        return from(EMPTY_METADATA, data);
    }

    public static Variant ofTimestampMicrosNtz(long value)
    {
        Slice data = Slices.allocate(ENCODED_TIMESTAMP_SIZE);
        encodeTimestampMicrosNtz(value, data, 0);
        return from(EMPTY_METADATA, data);
    }

    public static Variant ofTimestampMicrosNtz(LocalDateTime value)
    {
        Slice data = Slices.allocate(ENCODED_TIMESTAMP_SIZE);
        encodeTimestampMicrosNtz(value, data, 0);
        return from(EMPTY_METADATA, data);
    }

    public static Variant ofTimestampNanosNtz(long value)
    {
        Slice data = Slices.allocate(ENCODED_TIMESTAMP_SIZE);
        encodeTimestampNanosNtz(value, data, 0);
        return from(EMPTY_METADATA, data);
    }

    public static Variant ofTimestampNanosNtz(LocalDateTime value)
    {
        Slice data = Slices.allocate(ENCODED_TIMESTAMP_SIZE);
        encodeTimestampNanosNtz(value, data, 0);
        return from(EMPTY_METADATA, data);
    }

    public static Variant ofBinary(Slice value)
    {
        Slice data = Slices.allocate(encodedBinarySize(value.length()));
        encodeBinary(value, data, 0);
        return from(EMPTY_METADATA, data);
    }

    public static Variant ofString(String value)
    {
        return ofString(utf8Slice(value));
    }

    public static Variant ofString(Slice value)
    {
        Slice data = Slices.allocate(encodedStringSize(value.length()));
        encodeString(value, data, 0);
        return from(EMPTY_METADATA, data);
    }

    public static Variant ofUuid(UUID value)
    {
        Slice data = Slices.allocate(ENCODED_UUID_SIZE);
        encodeUuid(value, data, 0);
        return from(EMPTY_METADATA, data);
    }

    public static Variant ofUuid(Slice value)
    {
        Slice data = Slices.allocate(ENCODED_UUID_SIZE);
        encodeUuid(value, data, 0);
        return from(EMPTY_METADATA, data);
    }

    public static Variant ofArray(List<Variant> elements)
    {
        if (elements.isEmpty()) {
            return EMPTY_ARRAY;
        }
        Metadata.Builder metadataBuilder = Metadata.builder();
        List<VariantFieldRemapper> remappers = elements.stream()
                .map(variant -> VariantFieldRemapper.create(variant, metadataBuilder))
                .toList();

        // finalize the metadata and remappers
        SortedMetadata sortedMetadata = metadataBuilder.buildSorted();
        IntUnaryOperator sortedFieldIdMapping = sortedMetadata.sortedFieldIdMapping();
        remappers.forEach(remapper -> remapper.finalize(sortedFieldIdMapping));

        // allocate the output slice
        int totalSize = remappers.stream()
                .mapToInt(VariantFieldRemapper::size)
                .sum();
        Slice output = Slices.allocate(encodedArraySize(elements.size(), totalSize));

        // write array header
        int written = encodeArrayHeading(elements.size(), index -> remappers.get(index).size(), output, 0);

        // write remapped variants
        for (VariantFieldRemapper remapper : remappers) {
            remapper.write(output, written);
            written += remapper.size();
        }
        verify(written == output.length(), "Encoded size does not match expected size");
        return new Variant(output, sortedMetadata.metadata(), BasicType.ARRAY, null);
    }

    public static Variant ofObject(Map<Slice, Variant> fields)
    {
        if (fields.isEmpty()) {
            return EMPTY_OBJECT;
        }

        Metadata.Builder metadataBuilder = Metadata.builder();
        int[] fieldIds = fields.keySet().stream().mapToInt(metadataBuilder::addFieldName).toArray();
        List<VariantFieldRemapper> remappers = fields.values().stream().map(variant -> VariantFieldRemapper.create(variant, metadataBuilder)).toList();

        // finalize the metadata and remappers
        SortedMetadata sortedMetadata = metadataBuilder.buildSorted();
        IntUnaryOperator sortedFieldIdMapping = sortedMetadata.sortedFieldIdMapping();
        for (int i = 0; i < fieldIds.length; i++) {
            fieldIds[i] = sortedFieldIdMapping.applyAsInt(fieldIds[i]);
        }
        remappers.forEach(remapper -> remapper.finalize(sortedFieldIdMapping));

        // determine write order (objects must be written in lexicographical field-name order)
        // the high 32 bits are the fieldId, the low 32 bits are the original index
        long[] writeOrder = computeWriteOrder(fieldIds);
        for (int writeIndex = 1; writeIndex < writeOrder.length; writeIndex++) {
            verify(writeOrderFieldId(writeOrder, writeIndex) > writeOrderFieldId(writeOrder, writeIndex - 1), "Duplicate field IDs are not allowed in VARIANT objects");
        }

        // allocate the output slice
        int totalSize = remappers.stream()
                .mapToInt(VariantFieldRemapper::size)
                .sum();
        int maxFieldId = writeOrderFieldId(writeOrder, writeOrder.length - 1);
        Slice output = Slices.allocate(encodedObjectSize(maxFieldId, fieldIds.length, totalSize));

        // write object header
        int written = encodeObjectHeading(
                fieldIds.length,
                i -> writeOrderFieldId(writeOrder, i),
                i -> remappers.get(writeOrderOriginalIndex(writeOrder, i)).size(),
                output,
                0);

        // write remapped variants
        for (int i = 0; i < writeOrder.length; i++) {
            VariantFieldRemapper remapper = remappers.get(writeOrderOriginalIndex(writeOrder, i));
            remapper.write(output, written);
            written += remapper.size();
        }
        verify(written == output.length(), "Encoded size does not match expected size");
        return new Variant(output, sortedMetadata.metadata(), OBJECT, null);
    }

    public boolean isNull()
    {
        return primitiveType == PrimitiveType.NULL;
    }

    public boolean getBoolean()
    {
        if (primitiveType == PrimitiveType.BOOLEAN_TRUE) {
            return true;
        }
        if (primitiveType == PrimitiveType.BOOLEAN_FALSE) {
            return false;
        }
        throw new IllegalStateException("Expected primitive BOOLEAN but got " + primitiveType);
    }

    public byte getByte()
    {
        verifyType(PrimitiveType.INT8);
        return data.getByte(1);
    }

    public short getShort()
    {
        verifyType(PrimitiveType.INT16);
        return data.getShort(1);
    }

    public int getInt()
    {
        verifyType(PrimitiveType.INT32);
        return data.getInt(1);
    }

    public long getLong()
    {
        verifyType(PrimitiveType.INT64);
        return data.getLong(1);
    }

    public BigDecimal getDecimal()
    {
        checkState(primitiveType == PrimitiveType.DECIMAL4 ||
                        primitiveType == PrimitiveType.DECIMAL8 ||
                        primitiveType == PrimitiveType.DECIMAL16,
                () -> "Expected DECIMAL primitive but got %s".formatted(requireNonNullElse(primitiveType, basicType)));

        int scale = data.getByte(1);
        checkState(scale >= 0 && scale <= 38, () -> "Corrupt DECIMAL scale: %s".formatted(scale));

        return switch (primitiveType) {
            case DECIMAL4 -> {
                int unscaled = data.getInt(2);
                yield BigDecimal.valueOf(unscaled, scale);
            }
            case DECIMAL8 -> {
                long unscaled = data.getLong(2);
                yield BigDecimal.valueOf(unscaled, scale);
            }
            case DECIMAL16 -> {
                // 16-byte little-endian two's complement → BigInteger (big-endian)
                byte[] bigEndian = new byte[16];
                for (int i = 0; i < 16; i++) {
                    bigEndian[15 - i] = data.getByte(2 + i);
                }
                BigInteger unscaled = new BigInteger(bigEndian);
                yield new BigDecimal(unscaled, scale);
            }
            default -> throw new IllegalStateException("Expected DECIMAL primitive but got " + requireNonNullElse(primitiveType, basicType));
        };
    }

    public float getFloat()
    {
        verifyType(PrimitiveType.FLOAT);
        return data.getFloat(1);
    }

    public double getDouble()
    {
        verifyType(PrimitiveType.DOUBLE);
        return data.getDouble(1);
    }

    public int getDate()
    {
        verifyType(PrimitiveType.DATE);
        return data.getInt(1);
    }

    public LocalDate getLocalDate()
    {
        return LocalDate.ofEpochDay(getDate());
    }

    public long getTimeMicros()
    {
        checkState(primitiveType == PrimitiveType.TIME_NTZ_MICROS,
                () -> "Expected primitive TIME in microseconds but got %s".formatted(primitiveType));
        return data.getLong(1);
    }

    public LocalTime getLocalTime()
    {
        verifyType(PrimitiveType.TIME_NTZ_MICROS);
        long microsOfDay = getTimeMicros();
        long nanoOfDay = multiplyExact(microsOfDay, 1_000L);
        return LocalTime.ofNanoOfDay(nanoOfDay);
    }

    public long getTimestampMicros()
    {
        checkState(primitiveType == PrimitiveType.TIMESTAMP_UTC_MICROS || primitiveType == PrimitiveType.TIMESTAMP_NTZ_MICROS,
                () -> "Expected primitive TIMESTAMP in microseconds but got %s".formatted(primitiveType));
        return data.getLong(1);
    }

    public long getTimestampNanos()
    {
        checkState(primitiveType == PrimitiveType.TIMESTAMP_UTC_NANOS || primitiveType == PrimitiveType.TIMESTAMP_NTZ_NANOS,
                () -> "Expected primitive TIMESTAMP in nanoseconds but got %s".formatted(primitiveType));
        return data.getLong(1);
    }

    public Instant getInstant()
    {
        long seconds;
        int nanoOfSecond;
        if (primitiveType == PrimitiveType.TIMESTAMP_UTC_MICROS) {
            long micros = getTimestampMicros();
            seconds = Math.floorDiv(micros, 1_000_000);
            nanoOfSecond = toIntExact(Math.floorMod(micros, 1_000_000) * 1_000L);
        }
        else if (primitiveType == PrimitiveType.TIMESTAMP_UTC_NANOS) {
            long nanos = getTimestampNanos();
            seconds = Math.floorDiv(nanos, 1_000_000_000L);
            nanoOfSecond = (int) Math.floorMod(nanos, 1_000_000_000L);
        }
        else {
            throw new IllegalStateException("Expected primitive TIMESTAMP but got " + primitiveType);
        }
        return Instant.ofEpochSecond(seconds, nanoOfSecond);
    }

    public LocalDateTime getLocalDateTime()
    {
        long seconds;
        int nanoOfSecond;
        if (primitiveType == PrimitiveType.TIMESTAMP_NTZ_MICROS) {
            long micros = getTimestampMicros();
            seconds = Math.floorDiv(micros, 1_000_000);
            nanoOfSecond = toIntExact(Math.floorMod(micros, 1_000_000) * 1_000L);
        }
        else if (primitiveType == PrimitiveType.TIMESTAMP_NTZ_NANOS) {
            long nanos = getTimestampNanos();
            seconds = Math.floorDiv(nanos, 1_000_000_000L);
            nanoOfSecond = (int) Math.floorMod(nanos, 1_000_000_000L);
        }
        else {
            throw new IllegalStateException("Expected primitive TIMESTAMP but got " + primitiveType);
        }
        return LocalDateTime.ofEpochSecond(seconds, nanoOfSecond, ZoneOffset.UTC);
    }

    public Slice getBinary()
    {
        verifyType(PrimitiveType.BINARY);
        int length = data.getInt(1);
        return data.slice(5, length);
    }

    public Slice getString()
    {
        if (basicType == BasicType.SHORT_STRING) {
            int length = shortStringLength(data.getByte(0));
            return data.slice(1, length);
        }

        verifyType(PrimitiveType.STRING);
        int length = data.getInt(1);
        return data.slice(5, length);
    }

    public String getStringUtf8()
    {
        return getString().toStringUtf8();
    }

    public Slice getUuidSlice()
    {
        verifyType(PrimitiveType.UUID);
        return data.slice(1, 16);
    }

    public UUID getUuid()
    {
        verifyType(PrimitiveType.UUID);
        // UUID is 16-byte big-endian
        long mostSigBits = Long.reverseBytes(data.getLong(1));
        long leastSigBits = Long.reverseBytes(data.getLong(9));
        return new UUID(mostSigBits, leastSigBits);
    }

    public int getArrayLength()
    {
        verifyType(BasicType.ARRAY);
        int count = arrayIsLarge(data.getByte(0)) ? data.getInt(1) : (data.getByte(1) & 0xFF);
        checkState(count >= 0, () -> "Corrupt array count: " + count);
        return count;
    }

    public Variant getArrayElement(int index)
    {
        verifyType(BasicType.ARRAY);
        byte header = data.getByte(0);
        boolean large = arrayIsLarge(header);
        int offSize = arrayFieldOffsetSize(header);

        int count = large ? data.getInt(1) : (data.getByte(1) & 0xFF);
        Objects.checkIndex(index, count);

        int offsetsStart = 1 + (large ? 4 : 1);
        int valuesStart = offsetsStart + (count + 1) * offSize;

        int offsetPosition = offsetsStart + (index * offSize);
        int start = valuesStart + readOffset(data, offsetPosition, offSize);
        int end = valuesStart + readOffset(data, offsetPosition + offSize, offSize);
        return from(metadata, data.slice(start, end - start));
    }

    public Stream<Variant> arrayElements()
    {
        verifyType(BasicType.ARRAY);

        byte header = data.getByte(0);
        boolean large = arrayIsLarge(header);
        int offsetSize = arrayFieldOffsetSize(header);

        int count = large ? data.getInt(1) : (data.getByte(1) & 0xFF);

        int offsetsStart = 1 + (large ? 4 : 1);
        int valuesStart = offsetsStart + (count + 1) * offsetSize;

        return IntStream.range(0, count)
                .mapToObj(index -> {
                    int offsetPosition = offsetsStart + (index * offsetSize);
                    int start = valuesStart + readOffset(data, offsetPosition, offsetSize);
                    int end = valuesStart + readOffset(data, offsetPosition + offsetSize, offsetSize);
                    return from(metadata, data.slice(start, end - start));
                });
    }

    public int getObjectFieldCount()
    {
        verifyType(OBJECT);
        int count = objectIsLarge(data.getByte(0)) ? data.getInt(1) : (data.getByte(1) & 0xFF);
        checkState(count >= 0, () -> "Corrupt object field count: " + count);
        return count;
    }

    public Optional<Variant> getObjectField(int fieldId)
    {
        verifyType(OBJECT);
        checkArgument(fieldId >= 0 && fieldId < metadata.dictionarySize(),
                () -> "Invalid fieldId %d, valid range is [0, %d)".formatted(fieldId, metadata.dictionarySize()));
        byte header = data.getByte(0);
        boolean large = objectIsLarge(header);
        int idSize = objectFieldIdSize(header);
        int offSize = objectFieldOffsetSize(header);

        int count = large ? data.getInt(1) : (data.getByte(1) & 0xFF);
        checkState(count >= 0, () -> "Corrupt object field count: " + count);

        int idsStart = 1 + (large ? 4 : 1);
        int offsetsStart = idsStart + count * idSize;
        int valuesStart = offsetsStart + (count + 1) * offSize;

        int position = idsStart;
        for (int i = 0; i < count; i++, position += idSize) {
            int currentFieldId = readOffset(data, position, idSize);
            if (currentFieldId == fieldId) {
                int offsetPosition = offsetsStart + i * offSize;
                int start = valuesStart + readOffset(data, offsetPosition, offSize);
                int end = valuesStart + readOffset(data, offsetPosition + offSize, offSize);
                return Optional.of(from(metadata, data.slice(start, end - start)));
            }
        }
        return Optional.empty();
    }

    public Optional<Variant> getObjectField(Slice fieldName)
    {
        verifyType(OBJECT);

        byte header = data.getByte(0);
        boolean large = objectIsLarge(header);
        int idSize = objectFieldIdSize(header);
        int offSize = objectFieldOffsetSize(header);
        int count = large ? data.getInt(1) : (data.getByte(1) & 0xFF);
        checkState(count >= 0, () -> "Corrupt object field count: " + count);
        int idsStart = 1 + (large ? 4 : 1);
        int fieldIndex = findFieldIndex(fieldName, metadata, data, count, idsStart, idSize);
        if (fieldIndex < 0) {
            return Optional.empty();
        }

        int offsetsStart = idsStart + count * idSize;
        int offsetPosition = offsetsStart + fieldIndex * offSize;

        int valuesStart = offsetsStart + (count + 1) * offSize;
        int start = valuesStart + readOffset(data, offsetPosition, offSize);
        int end = valuesStart + readOffset(data, offsetPosition + offSize, offSize);
        return Optional.of(from(metadata, data.slice(start, end - start)));
    }

    public Stream<Slice> objectFieldNames()
    {
        verifyType(OBJECT);

        byte header = data.getByte(0);
        boolean large = objectIsLarge(header);
        int idSize = objectFieldIdSize(header);

        int count = large ? data.getInt(1) : (data.getByte(1) & 0xFF);

        int idsStart = 1 + (large ? 4 : 1);

        return IntStream.range(0, count)
                .mapToObj(i -> metadata.get(readOffset(data, idsStart + i * idSize, idSize)));
    }

    public Stream<Variant> objectValues()
    {
        verifyType(OBJECT);

        byte header = data.getByte(0);
        boolean large = objectIsLarge(header);
        int idSize = objectFieldIdSize(header);
        int offsetSize = objectFieldOffsetSize(header);

        int count = large ? data.getInt(1) : (data.getByte(1) & 0xFF);

        int idsStart = 1 + (large ? 4 : 1);
        int offsetsStart = idsStart + count * idSize;
        int valuesStart = offsetsStart + (count + 1) * offsetSize;

        return IntStream.range(0, count)
                .mapToObj(i -> {
                    int offsetPosition = offsetsStart + i * offsetSize;
                    int start = valuesStart + readOffset(data, offsetPosition, offsetSize);
                    int end = valuesStart + readOffset(data, offsetPosition + offsetSize, offsetSize);
                    return from(metadata, data.slice(start, end - start));
                });
    }

    public Stream<ObjectFieldIdValue> objectFields()
    {
        verifyType(OBJECT);

        byte header = data.getByte(0);
        boolean large = objectIsLarge(header);
        int idSize = objectFieldIdSize(header);
        int offsetSize = objectFieldOffsetSize(header);

        int count = large ? data.getInt(1) : (data.getByte(1) & 0xFF);

        int idsStart = 1 + (large ? 4 : 1);
        int offsetsStart = idsStart + count * idSize;
        int valuesStart = offsetsStart + (count + 1) * offsetSize;

        return IntStream.range(0, count)
                .mapToObj(i -> {
                    int fieldId = readOffset(data, idsStart + i * idSize, idSize);

                    int offsetPosition = offsetsStart + i * offsetSize;
                    int start = valuesStart + readOffset(data, offsetPosition, offsetSize);
                    int end = valuesStart + readOffset(data, offsetPosition + offsetSize, offsetSize);
                    Variant value = from(metadata, data.slice(start, end - start));

                    return new ObjectFieldIdValue(fieldId, value);
                });
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Variant rightValue)) {
            return false;
        }
        return VariantUtils.equals(
                metadata, data, 0,
                rightValue.metadata, rightValue.data, 0);
    }

    @Override
    public int hashCode()
    {
        return Long.hashCode(longHashCode());
    }

    public long longHashCode()
    {
        return VariantUtils.hashCode(metadata, data, 0);
    }

    /// Converts this Variant into a plain Java object graph.
    ///
    /// This method is intended for debugging and testing.
    /// The returned structure is composed of standard Java types and
    /// **unmodifiable** containers.
    ///
    /// - Objects become {@code Map<String, Object>}
    /// - Arrays become {@code List<Object>}
    /// - Nested {@code Variant} values are recursively converted
    ///
    /// ## Mapping
    ///
    /// | Variant kind | Variant type | Java type returned |
    /// |-------------|--------------|--------------------|
    /// | PRIMITIVE | NULL | {@code null} |
    /// | PRIMITIVE | BOOLEAN_TRUE / BOOLEAN_FALSE | {@code Boolean} |
    /// | PRIMITIVE | INT8 | {@code Byte} |
    /// | PRIMITIVE | INT16 | {@code Short} |
    /// | PRIMITIVE | INT32 | {@code Integer} |
    /// | PRIMITIVE | INT64 | {@code Long} |
    /// | PRIMITIVE | FLOAT | {@code Float} |
    /// | PRIMITIVE | DOUBLE | {@code Double} |
    /// | PRIMITIVE | DECIMAL4 / DECIMAL8 / DECIMAL16 | {@code BigDecimal} |
    /// | PRIMITIVE | DATE | {@code LocalDate} |
    /// | PRIMITIVE | TIME_NTZ_MICROS | {@code LocalTime} |
    /// | PRIMITIVE | TIMESTAMP_UTC_MICROS / TIMESTAMP_UTC_NANOS | {@code Instant} |
    /// | PRIMITIVE | TIMESTAMP_NTZ_MICROS / TIMESTAMP_NTZ_NANOS | {@code LocalDateTime} |
    /// | PRIMITIVE | BINARY | {@code Slice} |
    /// | PRIMITIVE | STRING | {@code String} |
    /// | PRIMITIVE | UUID | {@code UUID} |
    /// | SHORT_STRING | SHORT_STRING | {@code String} |
    /// | OBJECT | OBJECT | {@code Map<String, Object>} |
    /// | ARRAY | ARRAY | {@code List<Object>} |
    ///
    public Object toObject()
    {
        return switch (basicType()) {
            case PRIMITIVE -> switch (primitiveType()) {
                case NULL -> null;
                case BOOLEAN_TRUE -> true;
                case BOOLEAN_FALSE -> false;
                case INT8 -> getByte();
                case INT16 -> getShort();
                case INT32 -> getInt();
                case INT64 -> getLong();
                case DOUBLE -> getDouble();
                case DECIMAL4, DECIMAL8, DECIMAL16 -> getDecimal();
                case DATE -> getLocalDate();
                case TIMESTAMP_UTC_MICROS, TIMESTAMP_UTC_NANOS -> getInstant();
                case TIMESTAMP_NTZ_MICROS, TIMESTAMP_NTZ_NANOS -> getLocalDateTime();
                case FLOAT -> getFloat();
                case BINARY -> getBinary();
                case STRING -> getStringUtf8();
                case TIME_NTZ_MICROS -> getLocalTime();
                case UUID -> getUuid();
            };
            case SHORT_STRING -> getStringUtf8();
            case OBJECT -> {
                // values can be null, so we can't use the simple toMap collectors
                Map<String, Object> map = new HashMap<>(getObjectFieldCount());
                objectFields().forEach(field -> map.put(metadata().get(field.fieldId()).toStringUtf8(), field.value().toObject()));
                yield unmodifiableMap(map);
            }
            case ARRAY -> {
                // values can be null, so we can't use the simple toList collectors
                List<Object> array = new ArrayList<>(getArrayLength());
                arrayElements().map(Variant::toObject).forEach(array::add);
                yield unmodifiableList(array);
            }
        };
    }

    /// Creates a {@link Variant} from a plain Java object graph.
    ///
    /// This method is intended for debugging and testing.
    /// It performs multiple full-tree passes and is **not** optimized for performance.
    ///
    /// Supported container types are traversed recursively, and a single shared
    /// metadata dictionary is constructed for all objects in the graph.
    ///
    /// ## Supported input types
    ///
    /// - Objects become VARIANT objects
    /// - Lists become VARIANT arrays
    /// - Nested {@code Variant} values are rewritten into the resulting metadata
    ///
    /// ## Mapping
    ///
    /// | Java type | Variant kind | Variant type |
    /// |----------|--------------|--------------|
    /// | {@code null} | PRIMITIVE | NULL |
    /// | {@code Boolean} | PRIMITIVE | BOOLEAN_TRUE / BOOLEAN_FALSE |
    /// | {@code Byte} | PRIMITIVE | INT8 |
    /// | {@code Short} | PRIMITIVE | INT16 |
    /// | {@code Integer} | PRIMITIVE | INT32 |
    /// | {@code Long} | PRIMITIVE | INT64 |
    /// | {@code Float} | PRIMITIVE | FLOAT |
    /// | {@code Double} | PRIMITIVE | DOUBLE |
    /// | {@code BigDecimal} | PRIMITIVE | DECIMAL4 / DECIMAL8 / DECIMAL16 |
    /// | {@code LocalDate} | PRIMITIVE | DATE |
    /// | {@code LocalTime} | PRIMITIVE | TIME_NTZ_MICROS |
    /// | {@code Instant} | PRIMITIVE | TIMESTAMP_UTC_NANOS |
    /// | {@code LocalDateTime} | PRIMITIVE | TIMESTAMP_NTZ_NANOS |
    /// | {@code UUID} | PRIMITIVE | UUID |
    /// | {@code Slice} | PRIMITIVE | BINARY |
    /// | {@code byte[]} | PRIMITIVE | BINARY |
    /// | {@code String} | PRIMITIVE / SHORT_STRING | STRING |
    /// | {@code Map<String, Object>} | OBJECT | OBJECT |
    /// | {@code List<Object>} | ARRAY | ARRAY |
    /// | {@code Variant} | *rewritten* | *preserved semantics* |
    ///
    /// @throws IllegalArgumentException if an unsupported Java type is encountered
    /// @throws IllegalArgumentException if a map key is {@code null} or not a {@code String}
    public static Variant fromObject(Object value)
    {
        if (value == null) {
            return NULL_VALUE;
        }

        // Pass 1: collect all field names across the tree
        Metadata.Builder metadataBuilder = Metadata.builder();
        IdentityHashMap<Variant, VariantFieldRemapper> fieldRemappers = new IdentityHashMap<>();
        collectFieldNames(value, metadataBuilder, fieldRemappers);
        SortedMetadata sortedMetadata = metadataBuilder.buildSorted();

        fieldRemappers.values().forEach(remapper -> remapper.finalize(sortedMetadata.sortedFieldIdMapping()));
        Map<String, Integer> fieldIdByName = new HashMap<>(sortedMetadata.metadata().dictionarySize());
        for (int i = 0; i < sortedMetadata.metadata().dictionarySize(); i++) {
            fieldIdByName.put(sortedMetadata.metadata().get(i).toStringUtf8(), i);
        }

        // Pass 2: compute total size
        IdentityHashMap<Object, Integer> containerSizeCache = new IdentityHashMap<>();
        int totalSize = computeEncodedSize(value, fieldIdByName, fieldRemappers, containerSizeCache);

        // Pass 3: write
        Slice data = Slices.allocate(totalSize);
        int written = writeEncoded(value, fieldIdByName, data, 0, fieldRemappers, containerSizeCache);
        verify(written == totalSize, "Encoded size does not match expected size");
        return from(sortedMetadata.metadata(), data);
    }

    private static void collectFieldNames(Object value, Metadata.Builder metadataBuilder, IdentityHashMap<Variant, VariantFieldRemapper> fieldRemappers)
    {
        switch (value) {
            case null -> {}
            case Variant v -> fieldRemappers.computeIfAbsent(v, _ -> VariantFieldRemapper.create(v, metadataBuilder));
            case Map<?, ?> map -> {
                for (Object key : map.keySet()) {
                    metadataBuilder.addFieldName(utf8Slice(castMapKey(key)));
                }
                for (Object child : map.values()) {
                    collectFieldNames(child, metadataBuilder, fieldRemappers);
                }
            }
            case List<?> list -> {
                for (Object child : list) {
                    collectFieldNames(child, metadataBuilder, fieldRemappers);
                }
            }
            default -> {
                // primitives/leaf types: nothing to collect
            }
        }
    }

    private static int computeEncodedSize(
            Object value,
            Map<String, Integer> fieldIdByName,
            IdentityHashMap<Variant, VariantFieldRemapper> fieldRemappers,
            IdentityHashMap<Object, Integer> containerSizeCache)
    {
        return switch (value) {
            case null -> ENCODED_NULL_SIZE;
            case Variant v -> requireNonNull(fieldRemappers.get(v), "missing remapper").size();
            case Boolean _ -> ENCODED_BOOLEAN_SIZE;
            case Byte _ -> ENCODED_BYTE_SIZE;
            case Short _ -> ENCODED_SHORT_SIZE;
            case Integer _ -> ENCODED_INT_SIZE;
            case Long _ -> ENCODED_LONG_SIZE;
            case Float _ -> ENCODED_FLOAT_SIZE;
            case Double _ -> ENCODED_DOUBLE_SIZE;
            case BigDecimal decimal -> encodedDecimalSize(decimal);
            case LocalDate _ -> ENCODED_DATE_SIZE;
            case LocalTime _ -> ENCODED_TIME_SIZE;
            case Instant _, LocalDateTime _ -> ENCODED_TIMESTAMP_SIZE;
            case UUID _ -> ENCODED_UUID_SIZE;
            case Slice slice -> encodedBinarySize(slice.length());
            case byte[] bytes -> encodedBinarySize(bytes.length);
            case String s -> encodedStringSize(utf8Slice(s).length());
            case List<?> list -> containerSizeCache.computeIfAbsent(list, _ -> {
                        int totalElementsLength = 0;
                        for (Object element : list) {
                            totalElementsLength += computeEncodedSize(element, fieldIdByName, fieldRemappers, containerSizeCache);
                        }
                        return encodedArraySize(list.size(), totalElementsLength);
                    });
            case Map<?, ?> map -> containerSizeCache.computeIfAbsent(map, _ -> {
                        if (map.isEmpty()) {
                            return ENCODED_EMPTY_OBJECT_SIZE;
                        }

                        int maxFieldId = -1;
                        for (Object key : map.keySet()) {
                            maxFieldId = Math.max(maxFieldId, requireNonNull(fieldIdByName.get(castMapKey(key))));
                        }

                        int totalValuesLength = 0;
                        for (Object entry : map.values()) {
                            totalValuesLength += computeEncodedSize(entry, fieldIdByName, fieldRemappers, containerSizeCache);
                        }

                        return encodedObjectSize(maxFieldId, map.size(), totalValuesLength);
                    });
            default -> throw new IllegalArgumentException("Unsupported object type for VARIANT: " + value.getClass().getName());
        };
    }

    private static int writeEncoded(
            Object value,
            Map<String, Integer> fieldIdByName,
            Slice out,
            int offset,
            IdentityHashMap<Variant, VariantFieldRemapper> fieldRemappers,
            IdentityHashMap<Object, Integer> containerSizeCache)
    {
        return switch (value) {
            case null -> encodeNull(out, offset);
            case Variant v -> requireNonNull(fieldRemappers.get(v), "missing remapper").write(out, offset);
            case Boolean v -> encodeBoolean(v, out, offset);
            case Byte v -> encodeByte(v, out, offset);
            case Short v -> encodeShort(v, out, offset);
            case Integer v -> encodeInt(v, out, offset);
            case Long v -> encodeLong(v, out, offset);
            case Float v -> encodeFloat(v, out, offset);
            case Double v -> encodeDouble(v, out, offset);
            case BigDecimal decimal -> encodeDecimal(decimal, out, offset);
            case LocalDate date -> encodeDate(date, out, offset);
            case LocalTime time -> encodeTimeMicrosNtz(time, out, offset);
            case Instant instant -> encodeTimestampNanosUtc(instant, out, offset);
            case LocalDateTime dateTime -> encodeTimestampNanosNtz(dateTime, out, offset);
            case UUID uuid -> encodeUuid(uuid, out, offset);
            case Slice slice -> encodeBinary(slice, out, offset);
            case byte[] bytes -> encodeBinary(Slices.wrappedBuffer(bytes), out, offset);
            case String string -> encodeString(utf8Slice(string), out, offset);
            case List<?> list -> {
                int written = encodeArrayHeading(
                        list.size(),
                        i -> computeEncodedSize(list.get(i), fieldIdByName, fieldRemappers, containerSizeCache),
                        out,
                        offset);
                for (Object element : list) {
                    written += writeEncoded(element, fieldIdByName, out, offset + written, fieldRemappers, containerSizeCache);
                }
                yield written;
            }
            case Map<?, ?> map -> {
                int count = map.size();
                if (count == 0) {
                    // empty object (header only)
                    yield encodeObjectHeading(0, _ -> 0, _ -> 0, out, offset);
                }

                int[] fieldIds = new int[count];
                Object[] values = new Object[count];

                int i = 0;
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    fieldIds[i] = requireNonNull(fieldIdByName.get(castMapKey(entry.getKey())));
                    values[i] = entry.getValue();
                    i++;
                }

                // the high 32 bits are the fieldId, the low 32 bits are the original index
                long[] writeOrder = computeWriteOrder(fieldIds);
                for (int writeIndex = 1; writeIndex < writeOrder.length; writeIndex++) {
                    verify(writeOrderFieldId(writeOrder, writeIndex) > writeOrderFieldId(writeOrder, writeIndex - 1), "Duplicate field IDs are not allowed in VARIANT objects");
                }
                int written = encodeObjectHeading(
                        count,
                        index -> writeOrderFieldId(writeOrder, index),
                        index -> computeEncodedSize(values[writeOrderOriginalIndex(writeOrder, index)], fieldIdByName, fieldRemappers, containerSizeCache),
                        out,
                        offset);

                for (int writeOrderIndex = 0; writeOrderIndex < writeOrder.length; writeOrderIndex++) {
                    written += writeEncoded(values[writeOrderOriginalIndex(writeOrder, writeOrderIndex)], fieldIdByName, out, offset + written, fieldRemappers, containerSizeCache);
                }
                yield written;
            }
            default -> throw new IllegalArgumentException("Unsupported object type for VARIANT: " + value.getClass().getName());
        };
    }

    /// Returns an ordering of fields for writing. This result data is packed into a single
    /// long per field, where the high 32 bits are the fieldId and the low 32 bits are the
    /// original index.
    private static long[] computeWriteOrder(int[] fieldIds)
    {
        long[] order = new long[fieldIds.length];
        for (int i = 0; i < fieldIds.length; i++) {
            order[i] = (((long) fieldIds[i]) << 32) | (i & 0xffff_ffffL);
        }
        Arrays.sort(order);
        return order;
    }

    private static int writeOrderOriginalIndex(long[] writeOrder, int writeOrderIndex)
    {
        return (int) writeOrder[writeOrderIndex];
    }

    private static int writeOrderFieldId(long[] writeOrder, int writeOrderIndex)
    {
        return (int) (writeOrder[writeOrderIndex] >>> 32);
    }

    private static String castMapKey(Object key)
    {
        return switch (key) {
            case null -> throw new IllegalArgumentException("Map key is null");
            case String name -> name;
            case Slice name -> name.toStringUtf8();
            default -> throw new IllegalArgumentException("Map key must be a String: " + key.getClass().getName());
        };
    }

    private void verifyType(BasicType expected)
    {
        checkState(basicType == expected, () -> "Expected basic %s but got %s".formatted(expected, basicType));
    }

    private void verifyType(PrimitiveType expected)
    {
        checkState(primitiveType == expected, () -> "Expected primitive %s but got %s".formatted(expected, requireNonNullElse(primitiveType, basicType)));
    }

    @Override
    public String toString()
    {
        return "Variant[" +
                "basicType=" + basicType + ", " +
                "primitiveType=" + primitiveType + ']';
    }
}
