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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.iceberg.variants.ValueArray;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantObject;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.variant.Metadata.EMPTY_METADATA;
import static io.trino.spi.variant.VariantEncoder.ENCODED_DECIMAL16_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_DECIMAL4_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_DECIMAL8_SIZE;
import static io.trino.spi.variant.VariantEncoder.encodeDecimal16;
import static io.trino.spi.variant.VariantEncoder.encodeDecimal4;
import static io.trino.spi.variant.VariantEncoder.encodeDecimal8;
import static io.trino.spi.variant.VariantEncoder.encodeObject;
import static io.trino.spi.variant.VariantEncoder.encodedArraySize;
import static io.trino.spi.variant.VariantEncoder.encodedObjectSize;
import static io.trino.spi.variant.VariantUtils.verify;
import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestVariant
{
    @Test
    void testNullVariant()
    {
        assertPrimitiveEncoding(
                1,
                null,
                _ -> Variant.NULL_VALUE,
                variant -> {
                    assertThat(variant.isNull()).isTrue();
                    return null;
                },
                (_, variant, offset) -> VariantEncoder.encodeNull(variant, offset),
                serializeIcebergVariant(Variants.ofNull()));
    }

    @Test
    void testBoolean()
    {
        assertPrimitiveEncoding(
                1,
                true,
                Variant::ofBoolean,
                Variant::getBoolean,
                VariantEncoder::encodeBoolean,
                serializeIcebergVariant(Variants.of(true)));
        assertPrimitiveEncoding(
                1,
                false,
                Variant::ofBoolean,
                Variant::getBoolean,
                VariantEncoder::encodeBoolean,
                serializeIcebergVariant(Variants.of(false)));

        assertThat(Variant.ofBoolean(true)).isNotEqualTo(Variant.ofBoolean(false));
        assertThat(Variant.ofBoolean(true).longHashCode()).isNotEqualTo(Variant.ofBoolean(false).longHashCode());
    }

    @Test
    void testByte()
    {
        for (byte value : new byte[] {Byte.MIN_VALUE, -42, -1, 0, -42, 1, Byte.MAX_VALUE}) {
            assertPrimitiveEncoding(
                    2,
                    value,
                    Variant::ofByte,
                    Variant::getByte,
                    VariantEncoder::encodeByte,
                    serializeIcebergVariant(Variants.of(value)));

            assertEqualAndSameHash(Variant.ofByte(value), Variant.ofByte(value));
            assertEqualAndSameHash(Variant.ofByte(value), Variant.ofShort(value));
            assertEqualAndSameHash(Variant.ofByte(value), Variant.ofInt(value));
            assertEqualAndSameHash(Variant.ofByte(value), Variant.ofLong(value));
            assertEqualAndSameHash(Variant.ofByte(value), BigDecimal.valueOf(value));
        }
        assertThat(Variant.ofByte((byte) 0x12)).isNotEqualTo(Variant.ofByte((byte) 0x34));
        assertThat(Variant.ofByte((byte) 0x12).longHashCode()).isNotEqualTo(Variant.ofByte((byte) 0x34).longHashCode());
    }

    @Test
    void testShort()
    {
        for (short value : new short[] {Short.MIN_VALUE, Byte.MIN_VALUE, -42, -1, 0, -42, 1, Byte.MAX_VALUE, Short.MAX_VALUE}) {
            assertPrimitiveEncoding(
                    3,
                    value,
                    Variant::ofShort,
                    Variant::getShort,
                    VariantEncoder::encodeShort,
                    serializeIcebergVariant(Variants.of(value)));

            if ((byte) value == value) {
                assertEqualAndSameHash(Variant.ofShort(value), Variant.ofByte((byte) value));
            }
            else {
                assertNotEqualAndDifferentHash(Variant.ofShort(value), Variant.ofByte((byte) value));
            }
            assertEqualAndSameHash(Variant.ofShort(value), Variant.ofShort(value));
            assertEqualAndSameHash(Variant.ofShort(value), Variant.ofInt(value));
            assertEqualAndSameHash(Variant.ofShort(value), Variant.ofLong(value));
            assertEqualAndSameHash(Variant.ofShort(value), BigDecimal.valueOf(value));
        }

        assertThat(Variant.ofShort((short) 0x1234)).isNotEqualTo(Variant.ofShort((short) 0x5678));
        assertThat(Variant.ofShort((short) 0x1234).longHashCode()).isNotEqualTo(Variant.ofShort((short) 0x5678).longHashCode());
    }

    @Test
    void testInt()
    {
        for (int value : new int[] {Integer.MIN_VALUE, Short.MIN_VALUE, Byte.MIN_VALUE, -42, -1, 0, -42, 1, Byte.MAX_VALUE, Short.MAX_VALUE, Integer.MAX_VALUE}) {
            assertPrimitiveEncoding(
                    5,
                    value,
                    Variant::ofInt,
                    Variant::getInt,
                    VariantEncoder::encodeInt,
                    serializeIcebergVariant(Variants.of(value)));

            if ((byte) value == value) {
                assertEqualAndSameHash(Variant.ofInt(value), Variant.ofByte((byte) value));
            }
            else {
                assertNotEqualAndDifferentHash(Variant.ofInt(value), Variant.ofByte((byte) value));
            }
            if ((short) value == value) {
                assertEqualAndSameHash(Variant.ofInt(value), Variant.ofShort((short) value));
            }
            else {
                assertNotEqualAndDifferentHash(Variant.ofInt(value), Variant.ofShort((short) value));
            }
            assertEqualAndSameHash(Variant.ofInt(value), Variant.ofInt(value));
            assertEqualAndSameHash(Variant.ofInt(value), Variant.ofLong(value));
            assertEqualAndSameHash(Variant.ofInt(value), BigDecimal.valueOf(value));
        }

        assertThat(Variant.ofInt(0x12345678)).isNotEqualTo(Variant.ofInt(0x9ABCDEF0));
        assertThat(Variant.ofInt(0x12345678).longHashCode()).isNotEqualTo(Variant.ofInt(0x9ABCDEF0).longHashCode());
    }

    @Test
    void testLong()
    {
        assertPrimitiveEncoding(
                9,
                0x11223344_55667788L,
                Variant::ofLong,
                Variant::getLong,
                VariantEncoder::encodeLong,
                serializeIcebergVariant(Variants.of(0x11223344_55667788L)));

        for (long value : new long[] {
                Long.MIN_VALUE,
                Integer.MIN_VALUE,
                Short.MIN_VALUE,
                Byte.MIN_VALUE,
                -42L,
                -1L,
                0L,
                -42L,
                1L,
                Byte.MAX_VALUE,
                Short.MAX_VALUE,
                Integer.MAX_VALUE,
                Long.MAX_VALUE}) {
            assertPrimitiveEncoding(
                    9,
                    value,
                    Variant::ofLong,
                    Variant::getLong,
                    VariantEncoder::encodeLong,
                    serializeIcebergVariant(Variants.of(value)));

            if ((byte) value == value) {
                assertEqualAndSameHash(Variant.ofLong(value), Variant.ofByte((byte) value));
            }
            else {
                assertNotEqualAndDifferentHash(Variant.ofLong(value), Variant.ofByte((byte) value));
            }
            if ((short) value == value) {
                assertEqualAndSameHash(Variant.ofLong(value), Variant.ofShort((short) value));
            }
            else {
                assertNotEqualAndDifferentHash(Variant.ofLong(value), Variant.ofShort((short) value));
            }
            if ((int) value == value) {
                assertEqualAndSameHash(Variant.ofLong(value), Variant.ofInt((int) value));
            }
            else {
                assertNotEqualAndDifferentHash(Variant.ofLong(value), Variant.ofInt((int) value));
            }
            assertEqualAndSameHash(Variant.ofLong(value), Variant.ofLong(value));
            assertEqualAndSameHash(Variant.ofLong(value), BigDecimal.valueOf(value));
        }

        assertThat(Variant.ofLong(0x12345678_9ABCDEF0L)).isNotEqualTo(Variant.ofLong(42));
        assertThat(Variant.ofLong(0x12345678_9ABCDEF0L).longHashCode()).isNotEqualTo(Variant.ofLong(42).longHashCode());
    }

    @Test
    void testDecimal()
    {
        for (String string : List.of("0", "1", "-1", "123456789", "123456700", "-123456789", "-123456700")) {
            BigInteger unscaled = new BigInteger(string);
            for (int scale = 0; scale <= 9; scale++) {
                BigDecimal decimal4 = new BigDecimal(unscaled, scale);
                assertPrimitiveEncoding(
                        6,
                        decimal4,
                        Variant::ofDecimal,
                        Variant::getDecimal,
                        (BigDecimal value, Slice variant, int offset) -> encodeDecimal4(value.unscaledValue().intValueExact(), value.scale(), variant, offset),
                        serializeIcebergVariant(Variants.of(decimal4)));
                assertEqualAndSameHash(Variant.ofDecimal(decimal4), decimal4);
                BigDecimal stripTrailingZeros = decimal4.stripTrailingZeros();
                if (stripTrailingZeros.scale() < 0) {
                    stripTrailingZeros = stripTrailingZeros.setScale(0, RoundingMode.UNNECESSARY);
                }
                if (decimal4.scale() != stripTrailingZeros.scale()) {
                    assertEqualAndSameHash(Variant.ofDecimal(decimal4), stripTrailingZeros);
                }
            }
        }

        for (String string : List.of("1234567890", "123456789012345678", "123456789012345600", "-1234567890", "-123456789012345678", "-123456789012345600")) {
            BigInteger unscaled = new BigInteger(string);
            for (int scale = 0; scale <= 18; scale++) {
                BigDecimal decimal8 = new BigDecimal(unscaled, scale);
                assertPrimitiveEncoding(
                        10,
                        decimal8,
                        Variant::ofDecimal,
                        Variant::getDecimal,
                        (BigDecimal decimal, Slice slice, int offset) -> encodeDecimal8(decimal.unscaledValue().longValueExact(), decimal.scale(), slice, offset),
                        serializeIcebergVariant(Variants.of(decimal8)));
                assertEqualAndSameHash(Variant.ofDecimal(decimal8), decimal8);
                BigDecimal stripTrailingZeros = decimal8.stripTrailingZeros();
                if (stripTrailingZeros.scale() < 0) {
                    stripTrailingZeros = stripTrailingZeros.setScale(0, RoundingMode.UNNECESSARY);
                }
                if (decimal8.scale() != stripTrailingZeros.scale()) {
                    assertEqualAndSameHash(Variant.ofDecimal(decimal8), stripTrailingZeros);
                }
            }
        }

        for (String string : List.of(
                "1234567890123456789",
                "12345678901234567890123456789012345678",
                "12345678901234567890123456789012345600",
                "-1234567890123456789",
                "-12345678901234567890123456789012345678",
                "-12345678901234567890123456789012345600",
                "18446744073709551616",
                "9223372036854775808",
                "-9223372036854775809")) {
            BigInteger unscaled = new BigInteger(string);
            for (int scale = 0; scale <= 38; scale++) {
                BigDecimal decimal16 = new BigDecimal(unscaled, scale);
                assertPrimitiveEncoding(
                        18,
                        decimal16,
                        Variant::ofDecimal,
                        Variant::getDecimal,
                        (BigDecimal decimal, Slice slice, int offset) -> encodeDecimal16(decimal16.unscaledValue(), decimal.scale(), slice, offset),
                        serializeIcebergVariant(Variants.of(decimal16)));
                assertEqualAndSameHash(Variant.ofDecimal(decimal16), decimal16);
                BigDecimal stripTrailingZeros = decimal16.stripTrailingZeros();
                if (stripTrailingZeros.scale() < 0) {
                    stripTrailingZeros = stripTrailingZeros.setScale(0, RoundingMode.UNNECESSARY);
                }
                if (decimal16.scale() != stripTrailingZeros.scale()) {
                    assertEqualAndSameHash(Variant.ofDecimal(decimal16), stripTrailingZeros);
                }
            }
        }

        assertNotEqualAndDifferentHash(Variant.ofDecimal(new BigDecimal("1")), new BigDecimal("1e-18"));
        assertNotEqualAndDifferentHash(Variant.ofDecimal(new BigDecimal("1")), new BigDecimal("2e-19"));

        assertEqualAndSameHash(Variant.ofDecimal(new BigDecimal("1")), new BigDecimal("1.00"));
        assertEqualAndSameHash(Variant.ofDecimal(new BigDecimal("0")), new BigDecimal("0.00"));

        assertNotEqualAndDifferentHash(Variant.ofDecimal(new BigDecimal("123.45")), new BigDecimal("678.9"));
        assertNotEqualAndDifferentHash(Variant.ofDecimal(new BigDecimal("123.45")), new BigDecimal("678.90"));
        assertNotEqualAndDifferentHash(Variant.ofDecimal(new BigDecimal("123.45")), new BigDecimal("678.900"));

        assertThat(Variant.ofDecimal(new BigDecimal("1e-19"))).isNotEqualTo(Variant.ofDecimal(new BigDecimal("0")));
    }

    @Test
    void testFloat()
    {
        for (float value : new float[] {0.0f, -0.0f, 1.0f, -1.0f, 3.14f, -3.14f, Float.MAX_VALUE, Float.MIN_VALUE, Float.MIN_NORMAL, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY}) {
            assertPrimitiveEncoding(
                    5,
                    value,
                    Variant::ofFloat,
                    Variant::getFloat,
                    VariantEncoder::encodeFloat,
                    serializeIcebergVariant(Variants.of(value)));
        }

        assertEqualAndSameHash(Variant.ofFloat(0.0f), Variant.ofFloat(-0.0f));

        assertNotEqualAndDifferentHash(Variant.ofFloat(1.2345f), Variant.ofFloat(6.7890f));

        assertThat(Variant.ofFloat(Float.NaN)).isNotEqualTo(Variant.ofFloat(Float.NaN));
        assertThat(Variant.ofFloat(Float.NaN).longHashCode()).isEqualTo(Variant.ofFloat(Float.NaN).longHashCode());
    }

    @Test
    void testDouble()
    {
        for (double value : new double[] {0.0, -0.0, 1.0, -1.0, Math.PI, -Math.PI, Double.MAX_VALUE, Double.MIN_VALUE, Double.MIN_NORMAL, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}) {
            assertPrimitiveEncoding(
                    9,
                    value,
                    Variant::ofDouble,
                    Variant::getDouble,
                    VariantEncoder::encodeDouble,
                    serializeIcebergVariant(Variants.of(value)));
        }

        assertEqualAndSameHash(Variant.ofDouble(0.0), Variant.ofDouble(-0.0));

        assertNotEqualAndDifferentHash(Variant.ofDouble(1.2345), Variant.ofDouble(6.7890));

        assertThat(Variant.ofDouble(Double.NaN)).isNotEqualTo(Variant.ofDouble(Double.NaN));
        assertThat(Variant.ofDouble(Double.NaN).longHashCode()).isEqualTo(Variant.ofDouble(Double.NaN).longHashCode());
    }

    @Test
    void testDate()
    {
        for (int date : List.of(0, 1, -1, 36525, -36525, 20000, -20000, Integer.MAX_VALUE, Integer.MIN_VALUE)) {
            assertPrimitiveEncoding(
                    5,
                    toIntExact(date),
                    Variant::ofDate,
                    Variant::getDate,
                    VariantEncoder::encodeDate,
                    serializeIcebergVariant(Variants.ofDate(date)));
            assertPrimitiveEncoding(
                    5,
                    LocalDate.ofEpochDay(date),
                    Variant::ofDate,
                    Variant::getLocalDate,
                    VariantEncoder::encodeDate,
                    serializeIcebergVariant(Variants.ofDate(date)));
        }

        assertThat(Variant.ofDate(1000)).isNotEqualTo(Variant.ofDate(2000));
        assertThat(Variant.ofDate(1000).longHashCode()).isNotEqualTo(Variant.ofDate(2000).longHashCode());
    }

    @Test
    void testTimeMicros()
    {
        for (long micros : List.of(0L, 1L, 86399999999L)) {
            assertPrimitiveEncoding(
                    9,
                    micros,
                    Variant::ofTimeMicrosNtz,
                    Variant::getTimeMicros,
                    VariantEncoder::encodeTimeMicrosNtz,
                    serializeIcebergVariant(Variants.ofTime(micros)));
            assertPrimitiveEncoding(
                    9,
                    LocalTime.ofNanoOfDay(micros * 1_000L),
                    Variant::ofTimeMicrosNtz,
                    Variant::getLocalTime,
                    VariantEncoder::encodeTimeMicrosNtz,
                    serializeIcebergVariant(Variants.ofTime(micros)));
        }

        assertNotEqualAndDifferentHash(Variant.ofTimeMicrosNtz(86399999999L), Variant.ofTimeMicrosNtz(42L));
    }

    @Test
    void testTimestampMicros()
    {
        for (long micros : List.of(0L, 1L, -1L, 1625079045123456L, -1625079045123456L, Long.MAX_VALUE, Long.MIN_VALUE)) {
            long seconds = Math.floorDiv(micros, 1_000_000);
            int nanoOfSecond = toIntExact(Math.floorMod(micros, 1_000_000) * 1_000L);

            assertPrimitiveEncoding(
                    9,
                    micros,
                    Variant::ofTimestampMicrosUtc,
                    Variant::getTimestampMicros,
                    VariantEncoder::encodeTimestampMicrosUtc,
                    serializeIcebergVariant(Variants.ofTimestamptz(micros)));
            assertPrimitiveEncoding(
                    9,
                    Instant.ofEpochSecond(seconds, nanoOfSecond),
                    Variant::ofTimestampMicrosUtc,
                    Variant::getInstant,
                    VariantEncoder::encodeTimestampMicrosUtc,
                    serializeIcebergVariant(Variants.ofTimestamptz(micros)));

            assertPrimitiveEncoding(
                    9,
                    micros,
                    Variant::ofTimestampMicrosNtz,
                    Variant::getTimestampMicros,
                    VariantEncoder::encodeTimestampMicrosNtz,
                    serializeIcebergVariant(Variants.ofTimestampntz(micros)));
            assertPrimitiveEncoding(
                    9,
                    LocalDateTime.ofEpochSecond(seconds, nanoOfSecond, ZoneOffset.UTC),
                    Variant::ofTimestampMicrosNtz,
                    Variant::getLocalDateTime,
                    VariantEncoder::encodeTimestampMicrosNtz,
                    serializeIcebergVariant(Variants.ofTimestampntz(micros)));
        }

        assertThat(Variant.ofTimestampMicrosUtc(1625079045123456L)).isNotEqualTo(Variant.ofTimestampMicrosUtc(1625079045123457L));
        assertThat(Variant.ofTimestampMicrosUtc(1625079045123456L).longHashCode()).isNotEqualTo(Variant.ofTimestampMicrosUtc(1625079045123457L).longHashCode());

        assertThat(Variant.ofTimestampMicrosNtz(1625079045123456L)).isNotEqualTo(Variant.ofTimestampMicrosNtz(1625079045123457L));
        assertThat(Variant.ofTimestampMicrosNtz(1625079045123456L).longHashCode()).isNotEqualTo(Variant.ofTimestampMicrosNtz(1625079045123457L).longHashCode());
    }

    @Test
    void testTimestampNanosUtc()
    {
        for (long nanos : List.of(0L, 1L, -1L, 1625079045123456L, -1625079045123456L, Long.MAX_VALUE, Long.MIN_VALUE)) {
            long seconds = Math.floorDiv(nanos, 1_000_000_000L);
            int nanoOfSecond = (int) Math.floorMod(nanos, 1_000_000_000L);

            assertPrimitiveEncoding(
                    9,
                    nanos,
                    Variant::ofTimestampNanosUtc,
                    Variant::getTimestampNanos,
                    VariantEncoder::encodeTimestampNanosUtc,
                    serializeIcebergVariant(Variants.ofTimestamptzNanos(nanos)));
            assertPrimitiveEncoding(
                    9,
                    Instant.ofEpochSecond(seconds, nanoOfSecond),
                    Variant::ofTimestampNanosUtc,
                    Variant::getInstant,
                    VariantEncoder::encodeTimestampNanosUtc,
                    serializeIcebergVariant(Variants.ofTimestamptzNanos(nanos)));

            assertPrimitiveEncoding(
                    9,
                    nanos,
                    Variant::ofTimestampNanosNtz,
                    Variant::getTimestampNanos,
                    VariantEncoder::encodeTimestampNanosNtz,
                    serializeIcebergVariant(Variants.ofTimestampntzNanos(nanos)));
            assertPrimitiveEncoding(
                    9,
                    LocalDateTime.ofEpochSecond(seconds, nanoOfSecond, ZoneOffset.UTC),
                    Variant::ofTimestampNanosNtz,
                    Variant::getLocalDateTime,
                    VariantEncoder::encodeTimestampNanosNtz,
                    serializeIcebergVariant(Variants.ofTimestampntzNanos(nanos)));
        }

        assertThat(Variant.ofTimestampNanosUtc(1625079045123456789L)).isNotEqualTo(Variant.ofTimestampNanosUtc(1625079045123456790L));
        assertThat(Variant.ofTimestampNanosUtc(1625079045123456789L).longHashCode()).isNotEqualTo(Variant.ofTimestampNanosUtc(1625079045123456790L).longHashCode());

        assertThat(Variant.ofTimestampNanosNtz(1625079045123456789L)).isNotEqualTo(Variant.ofTimestampNanosNtz(1625079045123456790L));
        assertThat(Variant.ofTimestampNanosNtz(1625079045123456789L).longHashCode()).isNotEqualTo(Variant.ofTimestampNanosNtz(1625079045123456790L).longHashCode());
    }

    @Test
    void testBinary()
    {
        assertPrimitiveEncoding(
                5,
                Slices.EMPTY_SLICE,
                Variant::ofBinary,
                Variant::getBinary,
                VariantEncoder::encodeBinary,
                serializeIcebergVariant(Variants.of(ByteBuffer.allocate(0))));

        byte[] binaryData = new byte[] {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09};
        Slice binarySlice = wrappedBuffer(binaryData);

        assertPrimitiveEncoding(
                5 + binaryData.length,
                binarySlice,
                Variant::ofBinary,
                Variant::getBinary,
                VariantEncoder::encodeBinary,
                serializeIcebergVariant(Variants.of(ByteBuffer.wrap(binaryData))));

        assertThat(Variant.ofBinary(wrappedBuffer(new byte[] {0x01, 0x02}))).isNotEqualTo(Variant.ofBinary(wrappedBuffer(new byte[] {0x03, 0x04})));
        assertThat(Variant.ofBinary(wrappedBuffer(new byte[] {0x01, 0x02})).longHashCode()).isNotEqualTo(Variant.ofBinary(wrappedBuffer(new byte[] {0x03, 0x04})).longHashCode());
    }

    @Test
    void testString()
    {
        for (String text : List.of("", "Hello, Iceberg Variants!", Strings.repeat("Hello, Iceberg Variants!", 100))) {
            Slice textSlice = utf8Slice(text);
            int expectedSize = text.length() <= 63 ? 1 + textSlice.length() : 5 + textSlice.length();
            assertPrimitiveEncoding(
                    expectedSize,
                    textSlice,
                    Variant::ofString,
                    Variant::getString,
                    VariantEncoder::encodeString,
                    serializeIcebergVariant(Variants.of(text)));
        }

        assertThat(Variant.ofString("Variant A")).isNotEqualTo(Variant.ofString("Variant B"));
        assertThat(Variant.ofString("Variant A").longHashCode()).isNotEqualTo(Variant.ofString("Variant B").longHashCode());
    }

    @Test
    void testUuid()
    {
        String uuidString = "123e4567-e89b-12d3-a456-426614174000";
        UUID uuid = UUID.fromString(uuidString);
        assertPrimitiveEncoding(
                17,
                uuid,
                Variant::ofUuid,
                Variant::getUuid,
                VariantEncoder::encodeUuid,
                serializeIcebergVariant(Variants.ofUUID(uuidString)));

        Slice uuidSlice = Slices.allocate(16);
        uuidSlice.setLong(0, Long.reverseBytes(uuid.getMostSignificantBits()));
        uuidSlice.setLong(8, Long.reverseBytes(uuid.getLeastSignificantBits()));
        assertPrimitiveEncoding(
                17,
                uuidSlice,
                Variant::ofUuid,
                Variant::getUuidSlice,
                VariantEncoder::encodeUuid,
                serializeIcebergVariant(Variants.ofUUID(uuidString)));

        assertThat(Variant.ofUuid(UUID.fromString("123e4567-e89b-12d3-a456-426614174000")))
                .isNotEqualTo(Variant.ofUuid(UUID.fromString("223e4567-e89b-12d3-a456-426614174000")));
    }

    private static <T> void assertPrimitiveEncoding(int dataSize, T value, VariantFactory<T> factory, ValueGetter<T> getter, Encoder<T> encoder, Slice expectedEncoding)
    {
        Variant variant = factory.create(value);
        assertThat(getter.get(variant)).isEqualTo(value);
        // ensure variant equals itself
        assertEqualAndSameHash(variant, variant);

        Slice buffer = Slices.allocate(dataSize);
        assertThat(encoder.encode(value, buffer, 0)).isEqualTo(dataSize);
        assertThat(Variant.from(EMPTY_METADATA, buffer)).isEqualTo(variant);
        assertThat(buffer).isEqualTo(expectedEncoding);

        buffer = Slices.allocate(dataSize + 10);
        assertThat(encoder.encode(value, buffer, 5)).isEqualTo(dataSize);
        assertThat(Variant.from(EMPTY_METADATA, buffer.slice(5, dataSize))).isEqualTo(variant);
        assertThat(buffer.slice(5, dataSize)).isEqualTo(expectedEncoding);
        assertThat(buffer.slice(0, 5)).isEqualTo(Slices.allocate(5));
        assertThat(buffer.slice(5 + dataSize, 5)).isEqualTo(Slices.allocate(5));

        Variant copy = Variant.from(EMPTY_METADATA, buffer.slice(5, dataSize));
        assertEqualAndSameHash(copy, variant);
    }

    @Test
    void testArrayOneOfEach()
    {
        List<Variant> elements = new ArrayList<>();
        ValueArray array = Variants.array();

        elements.add(Variant.ofByte((byte) 0x12));
        array.add(Variants.of((byte) 0x12));
        elements.add(Variant.ofShort((short) 0x3456));
        array.add(Variants.of((short) 0x3456));
        elements.add(Variant.ofInt(0x789ABCDE));
        array.add(Variants.of(0x789ABCDE));
        elements.add(Variant.ofLong(0x1122334455667788L));
        array.add(Variants.of(0x1122334455667788L));
        elements.add(Variant.ofFloat(3.14f));
        array.add(Variants.of(3.14f));
        elements.add(Variant.ofDouble(Math.E));
        array.add(Variants.of(Math.E));
        elements.add(Variant.ofDate(LocalDate.of(2021, 5, 18)));
        array.add(Variants.ofDate(18765));
        elements.add(Variant.ofTimeMicrosNtz(86399999999L));
        array.add(Variants.ofTime(86399999999L));
        elements.add(Variant.ofTimestampMicrosUtc(Instant.ofEpochSecond(1625079045, 123456000)));
        array.add(Variants.ofTimestamptz(1625079045123456L));
        elements.add(Variant.ofTimestampNanosUtc(Instant.ofEpochSecond(1625079045, 123456789)));
        array.add(Variants.ofTimestamptzNanos(1625079045123456789L));
        elements.add(Variant.ofBinary(wrappedBuffer(new byte[] {0x0A, 0x0B, 0x0C})));
        array.add(Variants.of(ByteBuffer.wrap(new byte[] {0x0A, 0x0B, 0x0C})));
        elements.add(Variant.ofString("Iceberg Variants"));
        array.add(Variants.of("Iceberg Variants"));
        elements.add(Variant.ofUuid(UUID.fromString("123e4567-e89b-12d3-a456-426614174000")));
        array.add(Variants.ofUUID("123e4567-e89b-12d3-a456-426614174000"));

        assertArrayEncoding(elements, serializeIcebergVariant(array));
    }

    @Test
    void testArrayOffsetSizes()
    {
        int expectedOffsetSize = 1;
        // it is not possible to run the larger tests in CI due to memory constraints
        for (int size : List.of(0xFF / 9, 0xFFFF / 9/*, 0xFFFFFF / 9, (0xFFFFFF / 9) + 1*/)) {
            List<Variant> elements = new ArrayList<>(size);
            ValueArray array = Variants.array();
            int totalSize = 0;
            for (int i = 0; i < size; i++) {
                elements.add(Variant.ofLong(i));
                array.add(Variants.of((long) i));
                totalSize += 9;
            }
            assertThat(VariantUtils.getOffsetSize(totalSize)).isEqualTo(expectedOffsetSize);

            assertArrayEncoding(elements, serializeIcebergVariant(array));

            expectedOffsetSize++;
        }
    }

    private static void assertArrayEncoding(List<Variant> elements, Slice expectedEncoding)
    {
        Slice buffer = Slices.allocate(encodedArraySize(elements.size(), elements.stream().mapToInt(element -> element.data().length()).sum()));
        assertThat(VariantEncoder.encodeArray(elements.stream().map(Variant::data).toList(), buffer, 0)).isEqualTo(expectedEncoding.length());

        Variant variant = Variant.from(EMPTY_METADATA, buffer);
        assertThat(Variant.ofArray(elements)).isEqualTo(variant);
        assertThat(Variant.ofArray(elements).data()).isEqualTo(buffer);
        assertThat(Variant.fromObject(elements).toObject()).isEqualTo(variant.toObject());

        assertThat(variant.getArrayLength()).isEqualTo(elements.size());
        assertThat(variant.arrayElements().toList()).isEqualTo(elements);
        assertThat(IntStream.range(0, elements.size()).mapToObj(variant::getArrayElement).toList())
                .isEqualTo(elements);
        assertThat(buffer).isEqualTo(expectedEncoding);

        buffer = Slices.allocate(buffer.length() + 10);
        assertThat(VariantEncoder.encodeArray(elements.stream().map(Variant::data).toList(), buffer, 5)).isEqualTo(expectedEncoding.length());
        Variant bufferCopy = Variant.from(EMPTY_METADATA, buffer.slice(5, expectedEncoding.length()));
        assertThat(Variant.ofArray(elements)).isEqualTo(bufferCopy);
        assertThat(bufferCopy.getArrayLength()).isEqualTo(elements.size());
        assertThat(bufferCopy.arrayElements().toList()).isEqualTo(elements);
        assertThat(buffer.slice(5, expectedEncoding.length())).isEqualTo(expectedEncoding);

        assertEqualAndSameHash(bufferCopy, variant);

        List<Object> expectedJavaObjects = elements.stream()
                .map(Variant::toObject)
                .toList();
        assertThat(variant.toObject()).isEqualTo(expectedJavaObjects);
        assertThat(Variant.fromObject(expectedJavaObjects).toObject()).isEqualTo(expectedJavaObjects);

        Variant fromObjectCopy = Variant.fromObject(elements);
        assertThat(fromObjectCopy.toObject()).isEqualTo(expectedJavaObjects);
        assertEqualAndSameHash(fromObjectCopy, variant);

        assertThat(Variant.fromObject(List.of(expectedJavaObjects))).isNotEqualTo(variant);
        assertThat(Variant.fromObject(List.of(expectedJavaObjects)).longHashCode()).isNotEqualTo(variant.longHashCode());
    }

    @Test
    void testObjectOneOfEach()
    {
        List<Slice> fieldNames = new ArrayList<>();
        List<ObjectField> fields = new ArrayList<>();
        List<VariantValue> icebergValues = new ArrayList<>();

        int fieldId = 0;
        fieldNames.add(utf8Slice("byteField"));
        fields.add(new ObjectField(fieldId, Variant.ofByte((byte) 0x12)));
        icebergValues.add(Variants.of((byte) 0x12));
        fieldId++;

        fieldNames.add(utf8Slice("shortField"));
        fields.add(new ObjectField(fieldId, Variant.ofShort((short) 0x3456)));
        icebergValues.add(Variants.of((short) 0x3456));
        fieldId++;

        fieldNames.add(utf8Slice("intField"));
        fields.add(new ObjectField(fieldId, Variant.ofInt(0x789ABCDE)));
        icebergValues.add(Variants.of(0x789ABCDE));
        fieldId++;

        fieldNames.add(utf8Slice("longField"));
        fields.add(new ObjectField(fieldId, Variant.ofLong(0x1122334455667788L)));
        icebergValues.add(Variants.of(0x1122334455667788L));
        fieldId++;

        fieldNames.add(utf8Slice("floatField"));
        fields.add(new ObjectField(fieldId, Variant.ofFloat(3.14f)));
        icebergValues.add(Variants.of(3.14f));
        fieldId++;

        fieldNames.add(utf8Slice("doubleField"));
        fields.add(new ObjectField(fieldId, Variant.ofDouble(Math.E)));
        icebergValues.add(Variants.of(Math.E));
        fieldId++;

        fieldNames.add(utf8Slice("dateField"));
        fields.add(new ObjectField(fieldId, Variant.ofDate(LocalDate.of(2021, 5, 18))));
        icebergValues.add(Variants.ofDate(18765));
        fieldId++;

        fieldNames.add(utf8Slice("timeMicrosField"));
        fields.add(new ObjectField(fieldId, Variant.ofTimeMicrosNtz(86399999999L)));
        icebergValues.add(Variants.ofTime(86399999999L));
        fieldId++;

        fieldNames.add(utf8Slice("timestampMicrosUtcField"));
        fields.add(new ObjectField(fieldId, Variant.ofTimestampMicrosUtc(Instant.ofEpochSecond(1625079045, 123456000))));
        icebergValues.add(Variants.ofTimestamptz(1625079045123456L));
        fieldId++;

        fieldNames.add(utf8Slice("timestampNanosUtcField"));
        fields.add(new ObjectField(fieldId, Variant.ofTimestampNanosUtc(Instant.ofEpochSecond(1625079045, 123456789))));
        icebergValues.add(Variants.ofTimestamptzNanos(1625079045123456789L));
        fieldId++;

        fieldNames.add(utf8Slice("binaryField"));
        fields.add(new ObjectField(fieldId, Variant.ofBinary(wrappedBuffer(new byte[] {0x0A, 0x0B, 0x0C}))));
        icebergValues.add(Variants.of(ByteBuffer.wrap(new byte[] {0x0A, 0x0B, 0x0C})));
        fieldId++;
        fieldNames.add(utf8Slice("stringField"));
        fields.add(new ObjectField(fieldId, Variant.ofString("Iceberg Variants")));
        icebergValues.add(Variants.of("Iceberg Variants"));
        fieldId++;
        fieldNames.add(utf8Slice("uuidField"));
        fields.add(new ObjectField(fieldId, Variant.ofUuid(UUID.fromString("123e4567-e89b-12d3-a456-426614174000"))));
        icebergValues.add(Variants.ofUUID("123e4567-e89b-12d3-a456-426614174000"));

        TestingMetadata testingMetadata = TestingMetadata.of(fieldNames);
        assertObjectEncoding(testingMetadata.metadata(), fields, testingMetadata.icebergMetadata(), icebergValues);
    }

    @Test
    void testObjectOffsetSizes()
    {
        // Build a single large dictionary once that is shared across all tests
        TestingMetadata testingMetadata = TestingMetadata.of(IntStream.range(0, 0xFFFFFF + 1)
                .mapToObj("%08d"::formatted)
                .map(Slices::utf8Slice)
                .peek(fieldName -> assertThat(fieldName.length()).isEqualTo(8))
                .toList());

        int expectedFieldIdSize = 1;
        // it is not possible to run the larger tests in CI due to memory constraints
        for (int fieldCount : List.of(0xFF, 0xFFFF/*, 0xFFFFFF, 0xFFFFFF + 1*/)) {
            List<ObjectField> fields = new ArrayList<>();
            List<VariantValue> icebergValues = new ArrayList<>();

            for (int i = 0; i < fieldCount; i++) {
                fields.add(new ObjectField(i, Variant.ofLong(i)));
                icebergValues.add(Variants.of((long) i));
            }
            assertThat(VariantUtils.getOffsetSize(fieldCount)).isEqualTo(expectedFieldIdSize);

            assertObjectEncoding(testingMetadata.metadata(), fields, testingMetadata.icebergMetadata(), icebergValues);

            expectedFieldIdSize++;
        }
    }

    private static void assertObjectEncoding(Metadata metadata, List<ObjectField> fields, VariantMetadata variantMetadata, List<VariantValue> icebergValues)
    {
        // sort the fields by field name to determine expected order
        List<ObjectField> sortedFields = fields.stream()
                .sorted(Comparator.comparing(objectFieldIdValue -> metadata.get(objectFieldIdValue.fieldId())))
                .toList();

        // Iceberg code does not have a method to encode a variant object; it only supports deserialization.
        Slice buffer = encodeObjectWithSortedFields(sortedFields);

        List<String> sortedFieldNames = sortedFields.stream()
                .map(ObjectField::fieldId)
                .map(metadata::get)
                .map(Slice::toStringUtf8)
                .toList();

        Variant variant = Variant.from(metadata, buffer);
        assertThat(variant.getObjectFieldCount()).isEqualTo(sortedFields.size());
        assertThat(variant.objectFieldNames().map(Slice::toStringUtf8).toList()).isEqualTo(sortedFieldNames);
        assertThat(variant.objectValues().map(Variant::data).toList()).isEqualTo(sortedFields.stream().map(ObjectField::variantValue).toList());
        assertThat(variant.objectFields().map(field -> new ObjectField(field.fieldId(), field.value().data())).toList()).isEqualTo(sortedFields);

        // verify variant equals itself
        assertEqualAndSameHash(variant, variant);

        Map<String, Object> expectedJavaObject = new HashMap<>();
        for (ObjectField field : sortedFields) {
            expectedJavaObject.put(metadata.get(field.fieldId()).toStringUtf8(), Variant.from(metadata, field.variantValue()).toObject());
        }
        assertThat(variant.toObject()).isEqualTo(expectedJavaObject);
        assertThat(Variant.fromObject(expectedJavaObject).toObject()).isEqualTo(expectedJavaObject);

        Map<Slice, Variant> variantFields = new HashMap<>();
        for (ObjectField field : sortedFields) {
            variantFields.put(metadata.get(field.fieldId()), Variant.from(metadata, field.variantValue()));
        }
        Variant ofObjectCopy = Variant.ofObject(variantFields);
        assertThat(ofObjectCopy.toObject()).isEqualTo(expectedJavaObject);
        assertEqualAndSameHash(ofObjectCopy, variant);

        Variant fromObjectCopy = Variant.fromObject(variantFields);
        assertThat(fromObjectCopy.toObject()).isEqualTo(expectedJavaObject);
        assertEqualAndSameHash(fromObjectCopy, variant);

        if (sortedFields.size() < 500) {
            for (ObjectField field : sortedFields) {
                assertFieldLookup(field, variant, metadata);
            }
        }
        else {
            ThreadLocalRandom.current().ints(500, 0, sortedFields.size())
                    .mapToObj(sortedFields::get)
                    .forEach(field -> assertFieldLookup(field, variant, metadata));
        }

        VariantObject icebergObject = (VariantObject) VariantValue.from(variantMetadata, buffer.toByteBuffer().order(ByteOrder.LITTLE_ENDIAN));
        assertThat(icebergObject.numFields()).isEqualTo(sortedFields.size());
        assertThat(ImmutableList.copyOf(icebergObject.fieldNames())).isEqualTo(sortedFieldNames);
        if (sortedFields.size() < 500) {
            for (int i = 0; i < sortedFields.size(); i++) {
                assertThat(icebergObject.get(variantMetadata.get(i))).isEqualTo(icebergValues.get(i));
            }
        }
        else {
            ThreadLocalRandom.current().ints(500, 0, sortedFields.size())
                    .forEach(i -> assertThat(icebergObject.get(variantMetadata.get(i))).isEqualTo(icebergValues.get(i)));
        }

        Slice offsetBuffer = Slices.allocate(buffer.length() + 10);
        assertThat(encodeObject(
                sortedFields.size(),
                i -> sortedFields.get(i).fieldId(),
                i -> sortedFields.get(i).variantValue(),
                offsetBuffer,
                5))
                .isEqualTo(buffer.length());
        assertThat(offsetBuffer.slice(5, buffer.length())).isEqualTo(buffer);
        assertThat(offsetBuffer.slice(0, 5)).isEqualTo(Slices.allocate(5));
        assertThat(offsetBuffer.slice(5 + buffer.length(), 5)).isEqualTo(Slices.allocate(5));

        assertThat(Variant.fromObject(Map.of("all", variant))).isNotEqualTo(variant);
        assertThat(Variant.fromObject(Map.of("all", variant)).longHashCode()).isNotEqualTo(variant.longHashCode());
    }

    private static void assertFieldLookup(ObjectField field, Variant deserializedObjectVariant, Metadata metadata)
    {
        assertThat(deserializedObjectVariant.getObjectField(field.fieldId()).map(Variant::data)).contains(field.variantValue());
        assertThat(metadata.get(field.fieldId()).toStringUtf8()).isEqualTo(metadata.get(field.fieldId()).toStringUtf8());
        Slice fieldName = metadata.get(field.fieldId());
        assertThat(metadata.id(fieldName)).isEqualTo(field.fieldId());
        assertThat(deserializedObjectVariant.getObjectField(fieldName).map(Variant::data)).contains(field.variantValue());
    }

    private record TestingMetadata(Metadata metadata, VariantMetadata icebergMetadata)
    {
        private static TestingMetadata of(List<Slice> fieldNames)
        {
            Metadata metadata = Metadata.of(fieldNames);
            // this saves some memory, but iceberg does eventually inflate accessed field names to strings
            VariantMetadata icebergMetadata = Variants.metadata(metadata.toSlice().toByteBuffer().order(ByteOrder.LITTLE_ENDIAN));
            TestingMetadata result = new TestingMetadata(metadata, icebergMetadata);
            return result;
        }
    }

    private interface VariantFactory<T>
    {
        Variant create(T value);
    }

    private interface ValueGetter<T>
    {
        T get(Variant value);
    }

    private interface Encoder<T>
    {
        int encode(T value, Slice buffer, int offset);
    }

    private static Slice serializeIcebergVariant(VariantValue metadata)
    {
        int size = metadata.sizeInBytes();
        byte[] array = new byte[size];
        ByteBuffer valueBuf = ByteBuffer.wrap(array).order(ByteOrder.LITTLE_ENDIAN);
        metadata.writeTo(valueBuf, 0);
        return wrappedBuffer(array);
    }

    @Test
    void testFromObjectPrimitiveRoundTrip()
    {
        assertThat(Variant.fromObject(null)).isEqualTo(Variant.NULL_VALUE);

        assertThat(Variant.fromObject(true).toObject()).isEqualTo(true);
        assertThat(Variant.fromObject((byte) 12).toObject()).isEqualTo((byte) 12);
        assertThat(Variant.fromObject((short) 123).toObject()).isEqualTo((short) 123);
        assertThat(Variant.fromObject(123).toObject()).isEqualTo(123);
        assertThat(Variant.fromObject(123L).toObject()).isEqualTo(123L);
        assertThat(Variant.fromObject(1.5f).toObject()).isEqualTo(1.5f);
        assertThat(Variant.fromObject(1.5d).toObject()).isEqualTo(1.5d);

        BigDecimal decimal = new BigDecimal("1234.5678");
        assertThat(Variant.fromObject(decimal).toObject()).isEqualTo(decimal);

        LocalDate date = LocalDate.of(2024, 10, 24);
        assertThat(Variant.fromObject(date).toObject()).isEqualTo(date);

        Instant instant = Instant.parse("2024-10-24T12:34:56.123456789Z");
        assertThat(Variant.fromObject(instant).toObject()).isEqualTo(instant);

        LocalDateTime dateTime = LocalDateTime.parse("2024-10-24T12:34:56.123456789");
        assertThat(Variant.fromObject(dateTime).toObject()).isEqualTo(dateTime);

        UUID uuid = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
        assertThat(Variant.fromObject(uuid).toObject()).isEqualTo(uuid);

        Slice binary = wrappedBuffer(new byte[] {0x01, 0x02, 0x03});
        assertThat(Variant.fromObject(binary).toObject()).isEqualTo(binary);
        assertThat(Variant.fromObject(binary.getBytes()).toObject()).isEqualTo(binary);
    }

    @Test
    void testOfArrayAndFromObjectArray()
    {
        List<Variant> elements = List.of(Variant.ofInt(1), Variant.ofString("two"), Variant.NULL_VALUE, Variant.ofBoolean(true));

        Variant array = Variant.ofArray(elements);
        assertThat(array.getArrayLength()).isEqualTo(elements.size());
        assertThat(array.arrayElements().toList()).isEqualTo(elements);

        assertThat(Variant.fromObject(elements).toObject()).isEqualTo(array.toObject());
    }

    @Test
    void testOfObjectAndFromObjectObject()
    {
        Map<Slice, Variant> fields = new HashMap<>();
        fields.put(utf8Slice("b"), Variant.ofInt(2));
        fields.put(utf8Slice("a"), Variant.ofInt(1));
        fields.put(utf8Slice("c"), Variant.ofString("three"));

        Variant object = Variant.ofObject(fields);

        assertThat(object.getObjectFieldCount()).isEqualTo(3);
        assertThat(object.objectFieldNames().map(Slice::toStringUtf8).toList()).isEqualTo(List.of("a", "b", "c"));
        assertThat(object.toObject()).isEqualTo(Map.of("a", 1, "b", 2, "c", "three"));

        Map<String, Object> javaObject = Map.of("b", 2, "a", 1, "c", "three");
        assertThat(Variant.fromObject(javaObject).toObject()).isEqualTo(object.toObject());
    }

    @Test
    void testFromObjectMapKeyValidation()
    {
        Map<Object, Object> nonStringKey = new HashMap<>();
        nonStringKey.put(123, 1);
        assertThatThrownBy(() -> Variant.fromObject(nonStringKey))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Map key must be a String");

        Map<Object, Object> nullKey = new HashMap<>();
        nullKey.put(null, 1);
        assertThatThrownBy(() -> Variant.fromObject(nullKey))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Map key is null");
    }

    @Test
    void testEqualsShortStringAndLongStringAreEquivalent()
    {
        Slice utf8 = utf8Slice("abc");

        // Short-string encoding (VariantEncoder will pick short string)
        Slice shortEncoded = Slices.allocate(1 + utf8.length());
        assertThat(VariantEncoder.encodeString(utf8, shortEncoded, 0)).isEqualTo(1 + utf8.length());
        Variant shortVariant = Variant.from(EMPTY_METADATA, shortEncoded);

        // Long-string encoding forced: [header][int length][bytes]
        Slice longEncoded = Slices.allocate(1 + Integer.BYTES + utf8.length());
        longEncoded.setByte(0, Header.primitiveHeader(Header.PrimitiveType.STRING));
        longEncoded.setInt(1, utf8.length());
        longEncoded.setBytes(1 + Integer.BYTES, utf8);
        Variant longVariant = Variant.from(EMPTY_METADATA, longEncoded);

        assertEqualAndSameHash(shortVariant, longVariant);
    }

    @Test
    void testEqualsIntAndDecimalAreEquivalent()
    {
        Variant intVariant = Variant.ofByte((byte) 1);

        // decimal16 scale=2, unscaled=100
        Slice decimalEncoded = Slices.allocate(ENCODED_DECIMAL16_SIZE);
        encodeDecimal16(0, 100, 2, decimalEncoded, 0);
        Variant decimalVariant = Variant.from(EMPTY_METADATA, decimalEncoded);

        assertEqualAndSameHash(intVariant, decimalVariant);
    }

    @Test
    void testEqualsTimestampUtcMicrosAndNanosAreEquivalentWhenExactlyRepresentable()
    {
        long micros = 1_700_000_000_000_000L;
        long nanos = micros * 1_000L;

        Variant microsVariant = Variant.ofTimestampMicrosUtc(micros);
        Variant nanosVariant = Variant.ofTimestampNanosUtc(nanos);

        assertEqualAndSameHash(microsVariant, nanosVariant);
    }

    @Test
    void testEqualsTimestampUtcMicrosAndNanosNotEquivalentWhenNanosHasSubMicroRemainder()
    {
        long micros = 1_700_000_000_000_000L;
        long nanos = micros * 1_000L + 1; // not representable in micros

        Variant microsVariant = Variant.ofTimestampMicrosUtc(micros);
        Variant nanosVariant = Variant.ofTimestampNanosUtc(nanos);

        assertThat(microsVariant).isNotEqualTo(nanosVariant);
    }

    @Test
    void testFloatAndDoubleAreNotEquivalent()
    {
        Variant floatVariant = Variant.ofFloat(1.0f);
        Variant doubleVariant = Variant.ofDouble(1.0);

        assertThat(floatVariant).isNotEqualTo(doubleVariant);
    }

    @Test
    void testEqualsObjectWhenMetadataDictionariesDiffer()
    {
        Variant left = Variant.from(
                Metadata.of(List.of(utf8Slice("a"), utf8Slice("b"))),
                encodeObjectWithSortedFields(List.of(
                        new ObjectField(0, Variant.ofInt(123)),
                        new ObjectField(1, Variant.ofString("hello")))));
        Variant right = Variant.from(
                Metadata.of(List.of(utf8Slice("b"), utf8Slice("a"))),
                encodeObjectWithSortedFields(List.of(
                        new ObjectField(1, Variant.ofInt(123)),
                        new ObjectField(0, Variant.ofString("hello")))));

        // ensure we got different metadata dictionaries
        assertThat(left.metadata().get(0)).isNotEqualTo(right.metadata().get(0));
        assertThat(left.metadata().get(1)).isNotEqualTo(right.metadata().get(1));

        assertEqualAndSameHash(left, right);
    }

    private static void assertEqualAndSameHash(Variant leftValue, Variant rightValue)
    {
        assertThat(leftValue).isEqualTo(rightValue);
        assertThat(leftValue.longHashCode()).isEqualTo(rightValue.longHashCode());
        // some implementations have separate left and right side handling, so check both directions
        assertThat(rightValue).isEqualTo(leftValue);
        assertThat(rightValue.longHashCode()).isEqualTo(leftValue.longHashCode());

        Variant leftArray = Variant.ofArray(List.of(leftValue));
        Variant rightArray = Variant.ofArray(List.of(rightValue));
        assertThat(leftArray).isEqualTo(rightArray);
        assertThat(leftArray.longHashCode()).isEqualTo(rightArray.longHashCode());
        assertThat(rightArray).isEqualTo(leftArray);
        assertThat(rightArray.longHashCode()).isEqualTo(leftArray.longHashCode());

        Variant leftObject = Variant.ofObject(Map.of(utf8Slice("field"), leftValue));
        Variant rightObject = Variant.ofObject(Map.of(utf8Slice("field"), rightValue));
        assertThat(leftObject).isEqualTo(rightObject);
        assertThat(leftObject.longHashCode()).isEqualTo(rightObject.longHashCode());
        assertThat(rightObject).isEqualTo(leftObject);
        assertThat(rightObject.longHashCode()).isEqualTo(leftObject.longHashCode());
    }

    private static void assertEqualAndSameHash(Variant leftValue, BigDecimal bigDecimal)
    {
        for (Variant rightValue : allDecimalEncodings(bigDecimal)) {
            assertEqualAndSameHash(leftValue, rightValue);
        }
    }

    private static void assertNotEqualAndDifferentHash(Variant leftValue, Variant rightValue)
    {
        assertThat(leftValue).isNotEqualTo(rightValue);
        assertThat(leftValue.longHashCode()).isNotEqualTo(rightValue.longHashCode());
        // some implementations have separate left and right side handling, so check both directions
        assertThat(rightValue).isNotEqualTo(leftValue);
        assertThat(rightValue.longHashCode()).isNotEqualTo(leftValue.longHashCode());
    }

    private static void assertNotEqualAndDifferentHash(Variant leftValue, BigDecimal bigDecimal)
    {
        for (Variant rightValue : allDecimalEncodings(bigDecimal)) {
            assertNotEqualAndDifferentHash(leftValue, rightValue);
        }
    }

    private static List<Variant> allDecimalEncodings(BigDecimal bigDecimal)
    {
        List<Variant> variants = new ArrayList<>();
        BigInteger unscaled = bigDecimal.unscaledValue();
        int scale = bigDecimal.scale();
        if (unscaled.bitLength() < 32) {
            Slice data = Slices.allocate(ENCODED_DECIMAL4_SIZE);
            encodeDecimal4(unscaled.intValue(), scale, data, 0);
            variants.add(Variant.from(EMPTY_METADATA, data));
        }
        if (unscaled.bitLength() < 64) {
            Slice data = Slices.allocate(ENCODED_DECIMAL8_SIZE);
            encodeDecimal8(unscaled.longValue(), scale, data, 0);
            variants.add(Variant.from(EMPTY_METADATA, data));
        }
        if (unscaled.bitLength() > 128) {
            throw new IllegalArgumentException("Decimal precision out of range: " + unscaled.bitLength());
        }
        Slice data = Slices.allocate(ENCODED_DECIMAL16_SIZE);
        encodeDecimal16(unscaled, scale, data, 0);
        variants.add(Variant.from(EMPTY_METADATA, data));
        return variants;
    }

    // Builds a variant with the exact specified field order. Variants by spec are required to have fields sorted ordered by field name.
    // This method assumes that the caller has already sorted the fields by field name.
    // This method is necessary to build test variants without global sorting in the metadata dictionary, as all convenience methods
    // on Variant build a metadata dictionary with global sorting.
    private static Slice encodeObjectWithSortedFields(List<ObjectField> fields)
    {
        int expectedSize = encodedObjectSize(
                fields.stream()
                        .mapToInt(ObjectField::fieldId)
                        .max()
                        .orElse(0),
                fields.size(),
                fields.stream()
                        .mapToInt(field -> field.variantValue().length())
                        .sum());
        Slice output = Slices.allocate(expectedSize);

        int written = encodeObject(
                fields.size(),
                i -> fields.get(i).fieldId(),
                i -> fields.get(i).variantValue(),
                output,
                0);
        verify(written == expectedSize, "written size does not match expected size");
        return output;
    }

    private record ObjectField(int fieldId, Slice variantValue)
    {
        private ObjectField(int fieldId, Variant variant)
        {
            this(fieldId, variant.data());
            assertThat(variant.metadata()).isEqualTo(EMPTY_METADATA);
        }
    }
}
