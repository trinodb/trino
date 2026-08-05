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
package io.trino.jdbc;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.core.StreamWriteFeature;
import com.fasterxml.jackson.core.util.JsonRecyclerPools;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;

import java.io.IOException;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.multiplyExact;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

/**
 * JDBC representation for a {@code VARIANT} value encoded according to the Iceberg specification.
 * <p>
 * The value can be inspected with {@link #valueType()} and then read with the
 * corresponding typed accessor, or converted recursively to Java objects with
 * {@link #toObject()}.
 */
public final class Variant
{
    private static final JsonFactory JSON_FACTORY = jsonFactory();
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss");
    private static final VarHandle SHORT_HANDLE = MethodHandles.byteArrayViewVarHandle(short[].class, LITTLE_ENDIAN);
    private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, LITTLE_ENDIAN);
    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, LITTLE_ENDIAN);
    private static final long MICROS_PER_SECOND = 1_000_000L;
    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    private final Metadata metadata;
    private final byte[] valueBytes;
    private final int valueOffset;
    private final BasicType basicType;
    private final PrimitiveType primitiveType;
    private final ValueType valueType;

    private Variant(Metadata metadata, byte[] valueBytes, int valueOffset, int valueLength)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.valueBytes = requireNonNull(valueBytes, "valueBytes is null");
        checkArgument(valueLength > 0, "valueBytes is empty");
        checkArgument(valueOffset >= 0, "valueOffset is negative");
        checkArgument(valueOffset + valueLength <= valueBytes.length, "Invalid value range: offset=%s length=%s", valueOffset, valueLength);

        this.valueOffset = valueOffset;

        byte header = valueBytes[valueOffset];
        this.basicType = BasicType.fromHeader(header);
        this.primitiveType = (basicType == BasicType.PRIMITIVE) ? PrimitiveType.fromHeader(header) : null;
        this.valueType = ValueType.from(basicType, primitiveType);
    }

    public static Variant fromBytes(byte[] metadataBytes, byte[] valueBytes)
    {
        requireNonNull(metadataBytes, "metadataBytes is null");
        requireNonNull(valueBytes, "valueBytes is null");

        return new Variant(Metadata.fromBytes(metadataBytes), valueBytes, 0, valueBytes.length);
    }

    public byte[] getMetadataBytes()
    {
        return metadata.getBytes();
    }

    public byte[] getValueBytes()
    {
        int valueSize = valueSize(valueBytes, valueOffset);
        if (valueOffset == 0 && valueSize == valueBytes.length) {
            return valueBytes;
        }
        return copyBytes(valueBytes, valueOffset, valueSize);
    }

    /**
     * Returns the logical VARIANT value type.
     */
    public ValueType valueType()
    {
        return valueType;
    }

    public boolean isNull()
    {
        return valueType == ValueType.NULL;
    }

    public boolean getBoolean()
    {
        verifyValueType(ValueType.BOOLEAN);
        return primitiveType == PrimitiveType.BOOLEAN_TRUE;
    }

    public byte getByte()
    {
        verifyValueType(ValueType.INT8);
        return valueBytes[valueOffset + 1];
    }

    public short getShort()
    {
        verifyValueType(ValueType.INT16);
        return readShort(valueBytes, valueOffset + 1);
    }

    public int getInt()
    {
        verifyValueType(ValueType.INT32);
        return readInt(valueBytes, valueOffset + 1);
    }

    public long getLong()
    {
        verifyValueType(ValueType.INT64);
        return readLong(valueBytes, valueOffset + 1);
    }

    public BigDecimal getDecimal()
    {
        verifyValueType(ValueType.DECIMAL);

        int scale = valueBytes[valueOffset + 1] & 0xFF;
        switch (primitiveType) {
            case DECIMAL4:
                return BigDecimal.valueOf(readInt(valueBytes, valueOffset + 2), scale);
            case DECIMAL8:
                return BigDecimal.valueOf(readLong(valueBytes, valueOffset + 2), scale);
            case DECIMAL16: {
                byte[] bigEndian = new byte[16];
                LONG_HANDLE.set(bigEndian, 0, Long.reverseBytes(readLong(valueBytes, valueOffset + 2 + Long.BYTES)));
                LONG_HANDLE.set(bigEndian, Long.BYTES, Long.reverseBytes(readLong(valueBytes, valueOffset + 2)));
                return new BigDecimal(new BigInteger(bigEndian), scale);
            }
            default:
                throw new AssertionError("Unexpected decimal primitive type: " + primitiveType);
        }
    }

    public float getFloat()
    {
        verifyValueType(ValueType.FLOAT);
        return Float.intBitsToFloat(readInt(valueBytes, valueOffset + 1));
    }

    public double getDouble()
    {
        verifyValueType(ValueType.DOUBLE);
        return Double.longBitsToDouble(readLong(valueBytes, valueOffset + 1));
    }

    public int getDate()
    {
        verifyValueType(ValueType.DATE);
        return readInt(valueBytes, valueOffset + 1);
    }

    public LocalDate getLocalDate()
    {
        return LocalDate.ofEpochDay(getDate());
    }

    public long getTimeMicros()
    {
        verifyValueType(ValueType.TIME_NTZ_MICROS);
        return readLong(valueBytes, valueOffset + 1);
    }

    public LocalTime getLocalTime()
    {
        long nanoOfDay = multiplyExact(getTimeMicros(), 1_000L);
        return LocalTime.ofNanoOfDay(nanoOfDay);
    }

    public long getTimestampMicros()
    {
        checkState(
                valueType == ValueType.TIMESTAMP_UTC_MICROS || valueType == ValueType.TIMESTAMP_NTZ_MICROS,
                "Expected TIMESTAMP micros but got %s",
                valueType);
        return readLong(valueBytes, valueOffset + 1);
    }

    public long getTimestampNanos()
    {
        checkState(
                valueType == ValueType.TIMESTAMP_UTC_NANOS || valueType == ValueType.TIMESTAMP_NTZ_NANOS,
                "Expected TIMESTAMP nanos but got %s",
                valueType);
        return readLong(valueBytes, valueOffset + 1);
    }

    public Instant getInstant()
    {
        long seconds;
        int nanoOfSecond;
        if (valueType == ValueType.TIMESTAMP_UTC_MICROS) {
            long micros = getTimestampMicros();
            seconds = floorDiv(micros, 1_000_000);
            nanoOfSecond = floorMod(micros, 1_000_000) * 1_000;
        }
        else if (valueType == ValueType.TIMESTAMP_UTC_NANOS) {
            long nanos = getTimestampNanos();
            seconds = floorDiv(nanos, 1_000_000_000L);
            nanoOfSecond = floorMod(nanos, 1_000_000_000);
        }
        else {
            throw new IllegalStateException("Expected TIMESTAMP UTC but got " + valueType);
        }
        return Instant.ofEpochSecond(seconds, nanoOfSecond);
    }

    public LocalDateTime getLocalDateTime()
    {
        long seconds;
        int nanoOfSecond;
        if (valueType == ValueType.TIMESTAMP_NTZ_MICROS) {
            long micros = getTimestampMicros();
            seconds = floorDiv(micros, 1_000_000);
            nanoOfSecond = floorMod(micros, 1_000_000) * 1_000;
        }
        else if (valueType == ValueType.TIMESTAMP_NTZ_NANOS) {
            long nanos = getTimestampNanos();
            seconds = floorDiv(nanos, 1_000_000_000L);
            nanoOfSecond = floorMod(nanos, 1_000_000_000);
        }
        else {
            throw new IllegalStateException("Expected TIMESTAMP NTZ but got " + valueType);
        }
        return LocalDateTime.ofEpochSecond(seconds, nanoOfSecond, ZoneOffset.UTC);
    }

    public byte[] getBinary()
    {
        verifyValueType(ValueType.BINARY);
        int length = readInt(valueBytes, valueOffset + 1);
        return copyBytes(valueBytes, valueOffset + 5, length);
    }

    public String getString()
    {
        verifyValueType(ValueType.STRING);
        if (basicType == BasicType.SHORT_STRING) {
            return new String(valueBytes, valueOffset + 1, shortStringLength(valueBytes[valueOffset]), StandardCharsets.UTF_8);
        }

        int length = readInt(valueBytes, valueOffset + 1);
        return new String(valueBytes, valueOffset + 5, length, StandardCharsets.UTF_8);
    }

    public UUID getUuid()
    {
        verifyValueType(ValueType.UUID);
        long mostSigBits = Long.reverseBytes(readLong(valueBytes, valueOffset + 1));
        long leastSigBits = Long.reverseBytes(readLong(valueBytes, valueOffset + 9));
        return new UUID(mostSigBits, leastSigBits);
    }

    public int getArrayLength()
    {
        verifyValueType(ValueType.ARRAY);
        return arrayIsLarge(valueBytes[valueOffset]) ? readInt(valueBytes, valueOffset + 1) : valueBytes[valueOffset + 1] & 0xFF;
    }

    public Variant getArrayElement(int index)
    {
        verifyValueType(ValueType.ARRAY);
        int count = getArrayLength();
        checkElementIndex(index, count, "index");

        int offsetSize = arrayFieldOffsetSize(valueBytes[valueOffset]);
        int offsetsStart = valueOffset + 1 + (arrayIsLarge(valueBytes[valueOffset]) ? 4 : 1);
        int valuesStart = offsetsStart + (count + 1) * offsetSize;
        int start = valuesStart + readOffset(valueBytes, offsetsStart + index * offsetSize, offsetSize);
        int end = valuesStart + readOffset(valueBytes, offsetsStart + (index + 1) * offsetSize, offsetSize);
        return new Variant(metadata, valueBytes, start, end - start);
    }

    public List<Variant> getArrayElements()
    {
        int count = getArrayLength();
        List<Variant> elements = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            elements.add(getArrayElement(i));
        }
        return unmodifiableList(elements);
    }

    public int getObjectFieldCount()
    {
        verifyValueType(ValueType.OBJECT);
        return objectIsLarge(valueBytes[valueOffset]) ? readInt(valueBytes, valueOffset + 1) : valueBytes[valueOffset + 1] & 0xFF;
    }

    public Map<String, Variant> getObjectFields()
    {
        verifyValueType(ValueType.OBJECT);
        int count = getObjectFieldCount();
        int idSize = objectFieldIdSize(valueBytes[valueOffset]);
        int offsetSize = objectFieldOffsetSize(valueBytes[valueOffset]);
        int idsStart = valueOffset + 1 + (objectIsLarge(valueBytes[valueOffset]) ? 4 : 1);
        int offsetsStart = idsStart + count * idSize;
        int valuesStart = offsetsStart + (count + 1) * offsetSize;

        Map<String, Variant> fields = new LinkedHashMap<>(count);
        for (int index = 0; index < count; index++) {
            int fieldId = readOffset(valueBytes, idsStart + index * idSize, idSize);
            int fieldOffset = readOffset(valueBytes, offsetsStart + index * offsetSize, offsetSize);
            int start = valuesStart + fieldOffset;
            int size = valueSize(valueBytes, start);
            fields.put(metadata.get(fieldId), new Variant(metadata, valueBytes, start, size));
        }
        return unmodifiableMap(fields);
    }

    /**
     * Returns this value converted to the corresponding Java object representation.
     * <ul>
     * <li>{@link ValueType#NULL}: {@code null}</li>
     * <li>{@link ValueType#BOOLEAN}: {@link Boolean}</li>
     * <li>{@link ValueType#INT8}: {@link Byte}</li>
     * <li>{@link ValueType#INT16}: {@link Short}</li>
     * <li>{@link ValueType#INT32}: {@link Integer}</li>
     * <li>{@link ValueType#INT64}: {@link Long}</li>
     * <li>{@link ValueType#DOUBLE}: {@link Double}</li>
     * <li>{@link ValueType#DECIMAL}: {@link BigDecimal}</li>
     * <li>{@link ValueType#DATE}: {@link LocalDate}</li>
     * <li>{@link ValueType#TIMESTAMP_UTC_MICROS} and {@link ValueType#TIMESTAMP_UTC_NANOS}: {@link Instant}</li>
     * <li>{@link ValueType#TIMESTAMP_NTZ_MICROS} and {@link ValueType#TIMESTAMP_NTZ_NANOS}: {@link LocalDateTime}</li>
     * <li>{@link ValueType#FLOAT}: {@link Float}</li>
     * <li>{@link ValueType#BINARY}: {@code byte[]}</li>
     * <li>{@link ValueType#STRING}: {@link String}</li>
     * <li>{@link ValueType#TIME_NTZ_MICROS}: {@link LocalTime}</li>
     * <li>{@link ValueType#UUID}: {@link UUID}</li>
     * <li>{@link ValueType#ARRAY}: {@code List<Object>}</li>
     * <li>{@link ValueType#OBJECT}: {@code Map<String, Object>}</li>
     * </ul>
     */
    public Object toObject()
    {
        switch (valueType) {
            case NULL:
                return null;
            case BOOLEAN:
                return getBoolean();
            case INT8:
                return getByte();
            case INT16:
                return getShort();
            case INT32:
                return getInt();
            case INT64:
                return getLong();
            case DOUBLE:
                return getDouble();
            case DECIMAL:
                return getDecimal();
            case DATE:
                return getLocalDate();
            case TIMESTAMP_UTC_MICROS:
            case TIMESTAMP_UTC_NANOS:
                return getInstant();
            case TIMESTAMP_NTZ_MICROS:
            case TIMESTAMP_NTZ_NANOS:
                return getLocalDateTime();
            case FLOAT:
                return getFloat();
            case BINARY:
                return getBinary();
            case STRING:
                return getString();
            case TIME_NTZ_MICROS:
                return getLocalTime();
            case UUID:
                return getUuid();
            case ARRAY:
                List<Object> array = new ArrayList<>(getArrayLength());
                for (Variant element : getArrayElements()) {
                    array.add(element.toObject());
                }
                return unmodifiableList(array);
            case OBJECT:
                Map<String, Object> object = new LinkedHashMap<>(getObjectFieldCount());
                for (Map.Entry<String, Variant> entry : getObjectFields().entrySet()) {
                    object.put(entry.getKey(), entry.getValue().toObject());
                }
                return unmodifiableMap(object);
        }
        throw new AssertionError("Unhandled value type: " + valueType);
    }

    /**
     * Returns this value serialized to the same JSON representation used by the Trino protocol VARIANT JSON fallback.
     */
    public String toJson()
    {
        StringWriter writer = new StringWriter();
        try (JsonGenerator generator = JSON_FACTORY.createGenerator(writer)) {
            writeJson(generator);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to serialize VARIANT to JSON", e);
        }
        return writer.toString();
    }

    private void writeJson(JsonGenerator generator)
            throws IOException
    {
        switch (valueType) {
            case NULL:
                generator.writeNull();
                return;
            case BOOLEAN:
                generator.writeBoolean(getBoolean());
                return;
            case INT8:
                generator.writeNumber(getByte());
                return;
            case INT16:
                generator.writeNumber(getShort());
                return;
            case INT32:
                generator.writeNumber(getInt());
                return;
            case INT64:
                generator.writeNumber(getLong());
                return;
            case DOUBLE:
                generator.writeNumber(getDouble());
                return;
            case DECIMAL:
                generator.writeNumber(getDecimal());
                return;
            case DATE:
                generator.writeString(getLocalDate().toString());
                return;
            case TIMESTAMP_UTC_MICROS:
                generator.writeString(formatTimestamp(getTimestampMicros(), MICROS_PER_SECOND, 6, true));
                return;
            case TIMESTAMP_NTZ_MICROS:
                generator.writeString(formatTimestamp(getTimestampMicros(), MICROS_PER_SECOND, 6, false));
                return;
            case FLOAT:
                generator.writeNumber(getFloat());
                return;
            case BINARY:
                generator.writeBinary(getBinary());
                return;
            case STRING:
                generator.writeString(getString());
                return;
            case TIME_NTZ_MICROS:
                generator.writeString(formatTimeMicros(getTimeMicros()));
                return;
            case TIMESTAMP_UTC_NANOS:
                generator.writeString(formatTimestamp(getTimestampNanos(), NANOS_PER_SECOND, 9, true));
                return;
            case TIMESTAMP_NTZ_NANOS:
                generator.writeString(formatTimestamp(getTimestampNanos(), NANOS_PER_SECOND, 9, false));
                return;
            case UUID:
                generator.writeString(getUuid().toString());
                return;
            case ARRAY:
                generator.writeStartArray();
                for (Variant element : getArrayElements()) {
                    element.writeJson(generator);
                }
                generator.writeEndArray();
                return;
            case OBJECT:
                generator.writeStartObject();
                for (Map.Entry<String, Variant> entry : getObjectFields().entrySet()) {
                    generator.writeFieldName(entry.getKey());
                    entry.getValue().writeJson(generator);
                }
                generator.writeEndObject();
                return;
        }
        throw new AssertionError("Unhandled value type: " + valueType);
    }

    private void verifyValueType(ValueType expected)
    {
        checkState(valueType == expected, "Expected %s but got %s", expected, valueType);
    }

    @SuppressModernizer
    // JsonFactoryBuilder usage is intentional as JDBC should not depend on plugin-toolkit's JsonUtils.
    private static JsonFactory jsonFactory()
    {
        return new JsonFactoryBuilder()
                .streamReadConstraints(StreamReadConstraints.builder()
                        .maxStringLength(Integer.MAX_VALUE)
                        .maxNestingDepth(Integer.MAX_VALUE)
                        .maxNumberLength(Integer.MAX_VALUE)
                        .build())
                .enable(StreamReadFeature.USE_FAST_BIG_NUMBER_PARSER)
                .enable(StreamReadFeature.USE_FAST_DOUBLE_PARSER)
                .enable(StreamWriteFeature.USE_FAST_DOUBLE_WRITER)
                .recyclerPool(JsonRecyclerPools.threadLocalPool())
                .build();
    }

    private static String formatTimeMicros(long micros)
    {
        long secondsOfDay = micros / MICROS_PER_SECOND;
        int microsOfSecond = (int) (micros % MICROS_PER_SECOND);
        int hours = (int) (secondsOfDay / 3_600);
        int minutes = (int) ((secondsOfDay / 60) % 60);
        int seconds = (int) (secondsOfDay % 60);

        StringBuilder builder = new StringBuilder("HH:mm:ss.SSSSSS".length());
        appendFixedWidth(builder, hours, 2);
        builder.append(':');
        appendFixedWidth(builder, minutes, 2);
        builder.append(':');
        appendFixedWidth(builder, seconds, 2);
        builder.append('.');
        appendFixedWidth(builder, microsOfSecond, 6);
        return builder.toString();
    }

    private static String formatTimestamp(long value, long unitsPerSecond, int fractionalSecondsLength, boolean withTimeZone)
    {
        long epochSecond = floorDiv(value, unitsPerSecond);
        long fractionalSeconds = floorMod(value, unitsPerSecond);
        LocalDateTime dateTime = LocalDateTime.ofEpochSecond(epochSecond, 0, ZoneOffset.UTC);

        StringBuilder builder = new StringBuilder("yyyy-MM-dd HH:mm:ss.SSSSSSSSS UTC".length());
        TIMESTAMP_FORMATTER.formatTo(dateTime, builder);
        appendFraction(builder, fractionalSeconds, fractionalSecondsLength);
        if (withTimeZone) {
            builder.append(" UTC");
        }
        return builder.toString();
    }

    private static void appendFraction(StringBuilder builder, long fractionalSeconds, int fractionalSecondsLength)
    {
        if (fractionalSecondsLength == 0) {
            return;
        }

        builder.append('.');
        appendFixedWidth(builder, fractionalSeconds, fractionalSecondsLength);
    }

    private static void appendFixedWidth(StringBuilder builder, long value, int width)
    {
        checkArgument(value >= 0, "value is negative: %s", value);
        String text = Long.toString(value);
        checkArgument(text.length() <= width, "value does not fit in %s digits: %s", width, value);
        builder.append("0".repeat(width - text.length()));
        builder.append(text);
    }

    private static int valueSize(byte[] data, int offset)
    {
        byte header = data[offset];
        BasicType basicType = BasicType.fromHeader(header);
        if (basicType == BasicType.PRIMITIVE) {
            PrimitiveType primitiveType = PrimitiveType.fromHeader(header);
            switch (primitiveType) {
                case NULL:
                case BOOLEAN_TRUE:
                case BOOLEAN_FALSE:
                    return 1;
                case INT8:
                    return 2;
                case INT16:
                    return 3;
                case INT32:
                case FLOAT:
                case DATE:
                    return 5;
                case INT64:
                case DOUBLE:
                case TIME_NTZ_MICROS:
                case TIMESTAMP_UTC_MICROS:
                case TIMESTAMP_NTZ_MICROS:
                case TIMESTAMP_UTC_NANOS:
                case TIMESTAMP_NTZ_NANOS:
                    return 9;
                case DECIMAL4:
                    return 6;
                case DECIMAL8:
                    return 10;
                case DECIMAL16:
                    return 18;
                case BINARY:
                case STRING:
                    return 5 + readInt(data, offset + 1);
                case UUID:
                    return 17;
                default:
                    throw new AssertionError("Unexpected primitive type: " + primitiveType);
            }
        }
        if (basicType == BasicType.SHORT_STRING) {
            return 1 + shortStringLength(header);
        }
        if (basicType == BasicType.ARRAY) {
            boolean large = arrayIsLarge(header);
            int offsetSize = arrayFieldOffsetSize(header);
            int count = large ? readInt(data, offset + 1) : data[offset + 1] & 0xFF;
            int offsetsStart = offset + 1 + (large ? 4 : 1);
            int valuesStart = offsetsStart + (count + 1) * offsetSize;
            return valuesStart - offset + readOffset(data, offsetsStart + count * offsetSize, offsetSize);
        }

        boolean large = objectIsLarge(header);
        int idSize = objectFieldIdSize(header);
        int offsetSize = objectFieldOffsetSize(header);
        int count = large ? readInt(data, offset + 1) : data[offset + 1] & 0xFF;
        int idsStart = offset + 1 + (large ? 4 : 1);
        int offsetsStart = idsStart + count * idSize;
        int valuesStart = offsetsStart + (count + 1) * offsetSize;
        // Object field offsets do not need to be sorted; the final offset stores the total encoded object size.
        int objectSize = readOffset(data, offsetsStart + count * offsetSize, offsetSize);
        return valuesStart - offset + objectSize;
    }

    private static byte[] copyBytes(byte[] source, int offset, int length)
    {
        byte[] copy = new byte[length];
        System.arraycopy(source, offset, copy, 0, length);
        return copy;
    }

    private static int readOffset(byte[] data, int offset, int size)
    {
        switch (size) {
            case 1:
                return data[offset] & 0xFF;
            case 2:
                return readShort(data, offset) & 0xFFFF;
            case 3:
                return (data[offset] & 0xFF) |
                        ((data[offset + 1] & 0xFF) << 8) |
                        ((data[offset + 2] & 0xFF) << 16);
            case 4:
                return readInt(data, offset);
            default:
                throw new IllegalArgumentException("Unsupported offset size: " + size);
        }
    }

    private static short readShort(byte[] data, int offset)
    {
        return (short) SHORT_HANDLE.get(data, offset);
    }

    private static int readInt(byte[] data, int offset)
    {
        return (int) INT_HANDLE.get(data, offset);
    }

    private static long readLong(byte[] data, int offset)
    {
        return (long) LONG_HANDLE.get(data, offset);
    }

    private static int shortStringLength(byte header)
    {
        return (header & 0b1111_1100) >>> 2;
    }

    private static int objectFieldOffsetSize(byte header)
    {
        return ((header & 0b0000_1100) >>> 2) + 1;
    }

    private static int objectFieldIdSize(byte header)
    {
        return ((header & 0b0011_0000) >>> 4) + 1;
    }

    private static boolean objectIsLarge(byte header)
    {
        return (header & 0b0100_0000) != 0;
    }

    private static int arrayFieldOffsetSize(byte header)
    {
        return ((header & 0b0000_1100) >>> 2) + 1;
    }

    private static boolean arrayIsLarge(byte header)
    {
        return (header & 0b0001_0000) != 0;
    }

    private static int metadataVersion(byte header)
    {
        return header & 0b0000_1111;
    }

    private static int metadataOffsetSize(byte header)
    {
        return ((header & 0b1100_0000) >>> 6) + 1;
    }

    private static final class Metadata
    {
        private final byte[] metadataBytes;
        private final int dictionarySize;
        private final int offsetSize;

        private Metadata(byte[] metadataBytes, int dictionarySize, int offsetSize)
        {
            this.metadataBytes = metadataBytes;
            this.dictionarySize = dictionarySize;
            this.offsetSize = offsetSize;
        }

        public static Metadata fromBytes(byte[] metadataBytes)
        {
            requireNonNull(metadataBytes, "metadataBytes is null");
            if (metadataBytes.length == 0) {
                return new Metadata(metadataBytes, 0, 1);
            }
            checkArgument(metadataBytes.length >= 3, "metadataBytes is too short");

            byte header = metadataBytes[0];
            checkArgument(metadataVersion(header) == 1, "Unsupported metadata version: %s", metadataVersion(header));

            int offsetSize = metadataOffsetSize(header);
            int dictionarySize = readOffset(metadataBytes, 1, offsetSize);
            return new Metadata(metadataBytes, dictionarySize, offsetSize);
        }

        public byte[] getBytes()
        {
            return metadataBytes;
        }

        public String get(int id)
        {
            checkArgument(id >= 0 && id < dictionarySize, "Invalid field id: %s", id);

            int offsetsBase = 1 + offsetSize;
            int start = readOffset(metadataBytes, offsetsBase + id * offsetSize, offsetSize);
            int end = readOffset(metadataBytes, offsetsBase + (id + 1) * offsetSize, offsetSize);
            int dictionaryStart = offsetsBase + (dictionarySize + 1) * offsetSize;
            return new String(metadataBytes, dictionaryStart + start, end - start, StandardCharsets.UTF_8);
        }
    }

    private enum BasicType
    {
        PRIMITIVE,
        SHORT_STRING,
        OBJECT,
        ARRAY;

        private static BasicType fromHeader(byte header)
        {
            return values()[header & 0b0000_0011];
        }
    }

    private enum PrimitiveType
    {
        NULL,
        BOOLEAN_TRUE,
        BOOLEAN_FALSE,
        INT8,
        INT16,
        INT32,
        INT64,
        DOUBLE,
        DECIMAL4,
        DECIMAL8,
        DECIMAL16,
        DATE,
        TIMESTAMP_UTC_MICROS,
        TIMESTAMP_NTZ_MICROS,
        FLOAT,
        BINARY,
        STRING,
        TIME_NTZ_MICROS,
        TIMESTAMP_UTC_NANOS,
        TIMESTAMP_NTZ_NANOS,
        UUID;

        private static PrimitiveType fromHeader(byte header)
        {
            return values()[(header & 0b1111_1100) >>> 2];
        }
    }

    public enum ValueType
    {
        NULL,
        BOOLEAN,
        INT8,
        INT16,
        INT32,
        INT64,
        DOUBLE,
        DECIMAL,
        DATE,
        TIMESTAMP_UTC_MICROS,
        TIMESTAMP_NTZ_MICROS,
        FLOAT,
        BINARY,
        STRING,
        TIME_NTZ_MICROS,
        TIMESTAMP_UTC_NANOS,
        TIMESTAMP_NTZ_NANOS,
        UUID,
        OBJECT,
        ARRAY;

        private static ValueType from(BasicType basicType, PrimitiveType primitiveType)
        {
            switch (basicType) {
                case SHORT_STRING:
                    return STRING;
                case OBJECT:
                    return OBJECT;
                case ARRAY:
                    return ARRAY;
                case PRIMITIVE:
                    switch (primitiveType) {
                        case NULL:
                            return NULL;
                        case BOOLEAN_TRUE:
                        case BOOLEAN_FALSE:
                            return BOOLEAN;
                        case INT8:
                            return INT8;
                        case INT16:
                            return INT16;
                        case INT32:
                            return INT32;
                        case INT64:
                            return INT64;
                        case DOUBLE:
                            return DOUBLE;
                        case DECIMAL4:
                        case DECIMAL8:
                        case DECIMAL16:
                            return DECIMAL;
                        case DATE:
                            return DATE;
                        case TIMESTAMP_UTC_MICROS:
                            return TIMESTAMP_UTC_MICROS;
                        case TIMESTAMP_NTZ_MICROS:
                            return TIMESTAMP_NTZ_MICROS;
                        case FLOAT:
                            return FLOAT;
                        case BINARY:
                            return BINARY;
                        case STRING:
                            return STRING;
                        case TIME_NTZ_MICROS:
                            return TIME_NTZ_MICROS;
                        case TIMESTAMP_UTC_NANOS:
                            return TIMESTAMP_UTC_NANOS;
                        case TIMESTAMP_NTZ_NANOS:
                            return TIMESTAMP_NTZ_NANOS;
                        case UUID:
                            return UUID;
                    }
                    throw new AssertionError("Unhandled primitive type: " + primitiveType);
            }
            throw new AssertionError("Unhandled basic type: " + basicType);
        }
    }
}
