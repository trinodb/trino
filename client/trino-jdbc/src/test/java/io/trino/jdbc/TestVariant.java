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

import io.airlift.slice.Slice;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestVariant
{
    private static final Class<?> SPI_VARIANT_CLASS = loadClass("io.trino.spi.variant.Variant");
    private static final Class<?> SPI_METADATA_CLASS = loadClass("io.trino.spi.variant.Metadata");
    private static final Class<?> SPI_VARIANT_UTIL_CLASS = loadClass("io.trino.util.variant.VariantUtil");

    @Test
    public void testPrimitiveCompatibilityWithSpiVariant()
    {
        Variant nullVariant = spiCompatibleVariant("NULL_VALUE");
        assertThat(nullVariant.valueType()).isEqualTo(Variant.ValueType.NULL);
        assertThat(nullVariant.isNull()).isTrue();
        assertThat(nullVariant.toObject()).isNull();

        Variant trueVariant = spiCompatibleVariant("ofBoolean", true);
        assertThat(trueVariant.valueType()).isEqualTo(Variant.ValueType.BOOLEAN);
        assertThat(trueVariant.getBoolean()).isTrue();
        assertThat(trueVariant.toObject()).isEqualTo(true);

        Variant falseVariant = spiCompatibleVariant("ofBoolean", false);
        assertThat(falseVariant.getBoolean()).isFalse();

        Variant byteVariant = spiCompatibleVariant("ofByte", (byte) 0x7F);
        assertThat(byteVariant.valueType()).isEqualTo(Variant.ValueType.INT8);
        assertThat(byteVariant.getByte()).isEqualTo((byte) 0x7F);

        Variant shortVariant = spiCompatibleVariant("ofShort", (short) 0x1234);
        assertThat(shortVariant.valueType()).isEqualTo(Variant.ValueType.INT16);
        assertThat(shortVariant.getShort()).isEqualTo((short) 0x1234);

        Variant intVariant = spiCompatibleVariant("ofInt", 0x1234_5678);
        assertThat(intVariant.valueType()).isEqualTo(Variant.ValueType.INT32);
        assertThat(intVariant.getInt()).isEqualTo(0x1234_5678);

        Variant longVariant = spiCompatibleVariant("ofLong", 0x0102_0304_0506_0708L);
        assertThat(longVariant.valueType()).isEqualTo(Variant.ValueType.INT64);
        assertThat(longVariant.getLong()).isEqualTo(0x0102_0304_0506_0708L);

        Variant floatVariant = spiCompatibleVariant("ofFloat", 123.25f);
        assertThat(floatVariant.valueType()).isEqualTo(Variant.ValueType.FLOAT);
        assertThat(floatVariant.getFloat()).isEqualTo(123.25f);

        Variant doubleVariant = spiCompatibleVariant("ofDouble", -123.5d);
        assertThat(doubleVariant.valueType()).isEqualTo(Variant.ValueType.DOUBLE);
        assertThat(doubleVariant.getDouble()).isEqualTo(-123.5d);

        Variant decimal4Variant = spiCompatibleVariant("ofDecimal", new BigDecimal("123.45"));
        assertThat(decimal4Variant.valueType()).isEqualTo(Variant.ValueType.DECIMAL);
        assertThat(decimal4Variant.getDecimal()).isEqualByComparingTo("123.45");

        Variant decimal8Variant = spiCompatibleVariant("ofDecimal", new BigDecimal("-1234567890123.45"));
        assertThat(decimal8Variant.getDecimal()).isEqualByComparingTo("-1234567890123.45");

        Variant decimal16Variant = spiCompatibleVariant("ofDecimal", new BigDecimal("-123456789012345678901234567890.1234"));
        assertThat(decimal16Variant.getDecimal()).isEqualByComparingTo("-123456789012345678901234567890.1234");

        Variant dateVariant = spiCompatibleVariant("ofDate", LocalDate.of(1969, 12, 31));
        assertThat(dateVariant.valueType()).isEqualTo(Variant.ValueType.DATE);
        assertThat(dateVariant.getDate()).isEqualTo(-1);
        assertThat(dateVariant.getLocalDate()).isEqualTo(LocalDate.of(1969, 12, 31));

        Variant timeVariant = spiCompatibleVariant("ofTimeMicrosNtz", 12_345_678L);
        assertThat(timeVariant.valueType()).isEqualTo(Variant.ValueType.TIME_NTZ_MICROS);
        assertThat(timeVariant.getTimeMicros()).isEqualTo(12_345_678L);
        assertThat(timeVariant.getLocalTime()).isEqualTo(LocalTime.ofNanoOfDay(12_345_678_000L));

        UUID uuid = UUID.fromString("01234567-89ab-cdef-0123-456789abcdef");
        Variant uuidVariant = spiCompatibleVariant("ofUuid", uuid);
        assertThat(uuidVariant.valueType()).isEqualTo(Variant.ValueType.UUID);
        assertThat(uuidVariant.getUuid()).isEqualTo(uuid);
    }

    @Test
    public void testTimestampCompatibilityWithSpiVariant()
    {
        Variant utcMicros = spiCompatibleVariant("ofTimestampMicrosUtc", -1L);
        assertThat(utcMicros.valueType()).isEqualTo(Variant.ValueType.TIMESTAMP_UTC_MICROS);
        assertThat(utcMicros.getTimestampMicros()).isEqualTo(-1);
        assertThat(utcMicros.getInstant()).isEqualTo(Instant.ofEpochSecond(-1, 999_999_000));
        assertThat(utcMicros.toObject()).isEqualTo(Instant.ofEpochSecond(-1, 999_999_000));

        Variant utcNanos = spiCompatibleVariant("ofTimestampNanosUtc", -1L);
        assertThat(utcNanos.valueType()).isEqualTo(Variant.ValueType.TIMESTAMP_UTC_NANOS);
        assertThat(utcNanos.getTimestampNanos()).isEqualTo(-1);
        assertThat(utcNanos.getInstant()).isEqualTo(Instant.ofEpochSecond(-1, 999_999_999));

        Variant ntzMicros = spiCompatibleVariant("ofTimestampMicrosNtz", -1L);
        assertThat(ntzMicros.valueType()).isEqualTo(Variant.ValueType.TIMESTAMP_NTZ_MICROS);
        assertThat(ntzMicros.getTimestampMicros()).isEqualTo(-1);
        assertThat(ntzMicros.getLocalDateTime()).isEqualTo(LocalDateTime.ofEpochSecond(-1, 999_999_000, ZoneOffset.UTC));
        assertThat(ntzMicros.toObject()).isEqualTo(LocalDateTime.ofEpochSecond(-1, 999_999_000, ZoneOffset.UTC));

        Variant ntzNanos = spiCompatibleVariant("ofTimestampNanosNtz", -1L);
        assertThat(ntzNanos.valueType()).isEqualTo(Variant.ValueType.TIMESTAMP_NTZ_NANOS);
        assertThat(ntzNanos.getTimestampNanos()).isEqualTo(-1);
        assertThat(ntzNanos.getLocalDateTime()).isEqualTo(LocalDateTime.ofEpochSecond(-1, 999_999_999, ZoneOffset.UTC));
    }

    @Test
    public void testStringBinaryAndUuidCompatibilityWithSpiVariant()
    {
        Variant shortStringFixture = spiCompatibleVariant("ofString", "hello");
        Variant shortString = shortStringFixture;
        assertThat(shortString.valueType()).isEqualTo(Variant.ValueType.STRING);
        assertThat(shortString.getString()).isEqualTo("hello");
        assertThat(shortString.getValueBytes()).isEqualTo(shortStringFixture.getValueBytes());

        String longText = "x".repeat(80);
        Variant longStringFixture = spiCompatibleVariant("ofString", longText);
        Variant longString = longStringFixture;
        assertThat(longString.valueType()).isEqualTo(Variant.ValueType.STRING);
        assertThat(longString.getString()).isEqualTo(longText);
        assertThat(longString.getValueBytes()).isEqualTo(longStringFixture.getValueBytes());

        byte[] binaryPayload = new byte[] {0x01, 0x02, 0x03, (byte) 0xFF};
        Variant binaryVariant = spiCompatibleVariant("ofBinary", (Object) binaryPayload);
        assertThat(binaryVariant.valueType()).isEqualTo(Variant.ValueType.BINARY);
        assertThat(binaryVariant.getBinary()).containsExactly(binaryPayload);
        assertThat((byte[]) binaryVariant.toObject()).containsExactly(binaryPayload);
    }

    @Test
    public void testNestedContainerCompatibilityWithSpiVariant()
    {
        Variant alphaFixture = spiCompatibleVariant("ofInt", 111);
        Variant betaFixture = spiCompatibleVariant("ofArray", List.of(
                spiCompatibleVariant("ofBoolean", true),
                spiCompatibleVariant("ofString", "two"),
                spiCompatibleVariant("NULL_VALUE")));

        Map<String, Variant> fields = new LinkedHashMap<>();
        fields.put("beta", betaFixture);
        fields.put("alpha", alphaFixture);

        Variant variant = spiCompatibleVariant("ofObject", fields);

        assertThat(variant.valueType()).isEqualTo(Variant.ValueType.OBJECT);
        assertThat(variant.getObjectFieldCount()).isEqualTo(2);

        Map<String, Variant> objectFields = variant.getObjectFields();
        assertThat(objectFields.keySet()).containsExactly("alpha", "beta");
        assertThat(objectFields.get("alpha").getInt()).isEqualTo(111);
        assertThat(objectFields.get("alpha").getValueBytes()).isEqualTo(alphaFixture.getValueBytes());
        assertThat(objectFields.get("beta").valueType()).isEqualTo(Variant.ValueType.ARRAY);
        assertThat(objectFields.get("beta").getValueBytes()).isEqualTo(betaFixture.getValueBytes());
        assertThat(objectFields.get("beta").getArrayElements()).hasSize(3);
        assertThat(objectFields.get("beta").getArrayElement(0).getBoolean()).isTrue();
        assertThat(objectFields.get("beta").getArrayElement(1).getString()).isEqualTo("two");
        assertThat(objectFields.get("beta").getArrayElement(2).isNull()).isTrue();

        assertThatThrownBy(() -> objectFields.put("gamma", variant))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> objectFields.get("beta").getArrayElements().add(variant))
                .isInstanceOf(UnsupportedOperationException.class);

        Map<String, Object> object = castToMap(variant.toObject());
        assertThat(object).containsEntry("alpha", 111);
        assertThat(castToList(object.get("beta"))).containsExactly(true, "two", null);
        assertThatThrownBy(() -> object.put("gamma", 1))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testJsonCompatibilityWithServerVariant()
    {
        assertJson("NULL_VALUE", "null");
        assertJson("ofBoolean", "true", true);
        assertJson("ofInt", "123", 123);
        assertJson("ofDecimal", "123.45", new BigDecimal("123.45"));
        assertJson("ofString", "\"hello \\\"variant\\\"\"", "hello \"variant\"");
        assertJson("ofBinary", "\"AQID/w==\"", (Object) new byte[] {0x01, 0x02, 0x03, (byte) 0xFF});
        assertJson("ofDate", "\"2021-05-18\"", LocalDate.of(2021, 5, 18));
        assertJson("ofTimeMicrosNtz", "\"22:23:24.123456\"", 80_604_123_456L);
        assertJson("ofTimestampMicrosUtc", "\"1969-12-31 23:59:59.999999 UTC\"", -1L);
        assertJson("ofTimestampMicrosNtz", "\"1969-12-31 23:59:59.999999\"", -1L);
        assertJson("ofTimestampNanosUtc", "\"1969-12-31 23:59:59.999999999 UTC\"", -1L);
        assertJson("ofTimestampNanosNtz", "\"1969-12-31 23:59:59.999999999\"", -1L);

        Map<String, Variant> fields = new LinkedHashMap<>();
        fields.put("beta", spiCompatibleVariant("ofArray", List.of(
                spiCompatibleVariant("ofBoolean", true),
                spiCompatibleVariant("ofString", "two"),
                spiCompatibleVariant("NULL_VALUE"))));
        fields.put("alpha", spiCompatibleVariant("ofInt", 111));
        assertJson("ofObject", "{\"alpha\":111,\"beta\":[true,\"two\",null]}", fields);
    }

    @Test
    public void testLargeContainerCompatibilityWithSpiVariant()
    {
        List<Variant> arrayElements = new ArrayList<>();
        for (int index = 0; index < 300; index++) {
            arrayElements.add(spiCompatibleVariant("ofString", String.format("element-%03d-abcdefghij", index)));
        }

        Variant arrayVariant = spiCompatibleVariant("ofArray", arrayElements);
        assertThat(arrayVariant.valueType()).isEqualTo(Variant.ValueType.ARRAY);
        assertThat(arrayVariant.getArrayLength()).isEqualTo(300);
        assertThat(arrayVariant.getArrayElement(299).getString()).isEqualTo("element-299-abcdefghij");

        Map<String, Variant> objectFields = new LinkedHashMap<>();
        for (int index = 299; index >= 0; index--) {
            objectFields.put(String.format("field-%03d", index), spiCompatibleVariant("ofInt", index));
        }

        Variant objectVariant = spiCompatibleVariant("ofObject", objectFields);
        assertThat(objectVariant.valueType()).isEqualTo(Variant.ValueType.OBJECT);
        assertThat(objectVariant.getObjectFieldCount()).isEqualTo(300);
        assertThat(objectVariant.getObjectFields().get("field-299").getInt()).isEqualTo(299);
    }

    @Test
    public void testVariantUsesProvidedByteArrays()
    {
        Variant fixture = spiCompatibleVariant("ofObject", Map.of(
                "value", spiCompatibleVariant("ofInt", 123)));

        byte[] metadataBytes = fixture.getMetadataBytes();
        byte[] valueBytes = fixture.getValueBytes();

        Variant variant = Variant.fromBytes(metadataBytes, valueBytes);

        assertThat(variant.getObjectFields().get("value").getInt()).isEqualTo(123);
        assertThat(variant.getMetadataBytes()).isSameAs(metadataBytes);
        assertThat(variant.getValueBytes()).isSameAs(valueBytes);
    }

    @Test
    public void testWrongAccessorAndBoundsChecks()
    {
        Variant stringVariant = spiCompatibleVariant("ofString", "abc");
        assertThatThrownBy(stringVariant::getInt)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Expected INT32");

        Variant arrayVariant = spiCompatibleVariant("ofArray", List.of(
                spiCompatibleVariant("ofInt", 1)));
        assertThatThrownBy(() -> arrayVariant.getArrayElement(1))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> Variant.fromBytes(new byte[0], new byte[0]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("valueBytes is empty");
    }

    /**
     * Reflectively builds a JDBC variant using the SPI variant encoder. This must
     * be performed reflectively, because the SPI is compiled against a newer version
     * of Java.
     */
    private static Variant spiCompatibleVariant(String memberName, Object... arguments)
    {
        try {
            Object spiVariant = newSpiVariant(memberName, arguments);

            Object metadata = spiVariant.getClass().getMethod("metadata").invoke(spiVariant);
            Slice metadataSlice = (Slice) metadata.getClass().getMethod("toSlice").invoke(metadata);
            Slice dataSlice = (Slice) spiVariant.getClass().getMethod("data").invoke(spiVariant);

            Variant variant = Variant.fromBytes(metadataSlice.getBytes(), dataSlice.getBytes());
            assertThat(variant.getMetadataBytes()).isEqualTo(metadataSlice.getBytes());
            assertThat(variant.getValueBytes()).isEqualTo(dataSlice.getBytes());
            return variant;
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException("Failed to create JDBC Variant from SPI member " + memberName, e);
        }
    }

    private static void assertJson(String memberName, String expectedJson, Object... arguments)
    {
        Variant variant = spiCompatibleVariant(memberName, arguments);
        assertThat(variant.toJson()).isEqualTo(expectedJson);
        assertThat(variant.toJson()).isEqualTo(spiVariantJson(memberName, arguments));
    }

    private static String spiVariantJson(String memberName, Object... arguments)
    {
        try {
            Object spiVariant = newSpiVariant(memberName, arguments);
            Slice json = (Slice) SPI_VARIANT_UTIL_CLASS.getMethod("asJson", SPI_VARIANT_CLASS).invoke(null, spiVariant);
            return json.toStringUtf8();
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException("Failed to serialize SPI Variant " + memberName + " as JSON", e);
        }
    }

    private static Object newSpiVariant(String memberName, Object... arguments)
            throws ReflectiveOperationException
    {
        if (arguments.length == 0) {
            return SPI_VARIANT_CLASS.getField(memberName).get(null);
        }
        Method method = SPI_VARIANT_CLASS.getMethod(memberName, parameterTypes(arguments));
        return method.invoke(null, spiArguments(arguments));
    }

    private static Class<?>[] parameterTypes(Object[] arguments)
    {
        Class<?>[] parameterTypes = new Class<?>[arguments.length];
        for (int index = 0; index < arguments.length; index++) {
            Object argument = arguments[index];
            if (argument instanceof Boolean) {
                parameterTypes[index] = boolean.class;
            }
            else if (argument instanceof Byte) {
                parameterTypes[index] = byte.class;
            }
            else if (argument instanceof Short) {
                parameterTypes[index] = short.class;
            }
            else if (argument instanceof Integer) {
                parameterTypes[index] = int.class;
            }
            else if (argument instanceof Long) {
                parameterTypes[index] = long.class;
            }
            else if (argument instanceof Float) {
                parameterTypes[index] = float.class;
            }
            else if (argument instanceof Double) {
                parameterTypes[index] = double.class;
            }
            else if (argument instanceof byte[]) {
                parameterTypes[index] = Slice.class;
            }
            else if (argument instanceof List<?>) {
                parameterTypes[index] = List.class;
            }
            else if (argument instanceof Map<?, ?>) {
                parameterTypes[index] = Map.class;
            }
            else {
                parameterTypes[index] = argument.getClass();
            }
        }
        return parameterTypes;
    }

    private static Object[] spiArguments(Object[] arguments)
    {
        Object[] spiArguments = new Object[arguments.length];
        for (int index = 0; index < arguments.length; index++) {
            spiArguments[index] = toSpiArgument(arguments[index]);
        }
        return spiArguments;
    }

    private static Object toSpiArgument(Object argument)
    {
        if (argument instanceof byte[]) {
            return wrappedBuffer((byte[]) argument);
        }
        if (argument instanceof Variant) {
            Variant variant = (Variant) argument;
            try {
                Method metadataFrom = SPI_METADATA_CLASS.getMethod("from", Slice.class);
                Object metadata = metadataFrom.invoke(null, wrappedBuffer(variant.getMetadataBytes()));
                Method variantFrom = SPI_VARIANT_CLASS.getMethod("from", SPI_METADATA_CLASS, Slice.class);
                return variantFrom.invoke(null, metadata, wrappedBuffer(variant.getValueBytes()));
            }
            catch (ReflectiveOperationException e) {
                throw new RuntimeException("Failed to convert JDBC Variant back to SPI Variant", e);
            }
        }
        if (argument instanceof List<?>) {
            List<?> list = (List<?>) argument;
            return list.stream()
                    .map(TestVariant::toSpiArgument)
                    .collect(Collectors.toList());
        }
        if (argument instanceof Map<?, ?>) {
            Map<?, ?> map = (Map<?, ?>) argument;
            Map<Object, Object> converted = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                Object key = entry.getKey() instanceof String ? utf8Slice((String) entry.getKey()) : entry.getKey();
                converted.put(key, toSpiArgument(entry.getValue()));
            }
            return converted;
        }
        return argument;
    }

    private static Class<?> loadClass(String name)
    {
        try {
            return Class.forName(name);
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load class " + name, e);
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> castToMap(Object value)
    {
        return (Map<String, Object>) value;
    }

    @SuppressWarnings("unchecked")
    private static List<Object> castToList(Object value)
    {
        return (List<Object>) value;
    }
}
