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
package io.trino.type;

import com.google.common.collect.ImmutableSortedMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.operator.scalar.VarbinaryFunctions;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.SqlTime;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.SqlVarbinary;
import io.trino.spi.variant.Metadata;
import io.trino.spi.variant.ObjectFieldIdValue;
import io.trino.spi.variant.Variant;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.assertions.TrinoExceptionAssert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.variant.VariantEncoder.encodeObject;
import static io.trino.spi.variant.VariantEncoder.encodedObjectSize;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.math.BigDecimal.ONE;
import static java.math.RoundingMode.HALF_UP;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.assertj.core.api.InstanceOfAssertFactories.map;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
class TestVariantOperators
{
    private QueryAssertions assertions;

    @BeforeAll
    void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterAll
    void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    void testCastWithBoolean()
    {
        for (boolean value : new boolean[] {true, false}) {
            assertCastToVariant("BOOLEAN '%s'".formatted(value), value);
            assertCastFromVariant(Variant.ofBoolean(value), "BOOLEAN", value);
            assertCastFromVariant(Variant.ofString(value ? "true" : "false"), "BOOLEAN", value);
        }

        assertCastFromVariant(Variant.ofByte((byte) 0), "BOOLEAN", false);
        assertCastFromVariant(Variant.ofByte((byte) 7), "BOOLEAN", true);

        assertCastFromVariant(Variant.ofShort((short) 0), "BOOLEAN", false);
        assertCastFromVariant(Variant.ofShort((short) -3), "BOOLEAN", true);

        assertCastFromVariant(Variant.ofInt(0), "BOOLEAN", false);
        assertCastFromVariant(Variant.ofInt(42), "BOOLEAN", true);

        assertCastFromVariant(Variant.ofLong(0L), "BOOLEAN", false);
        assertCastFromVariant(Variant.ofLong(Long.MIN_VALUE), "BOOLEAN", true);

        assertCastFromVariant(Variant.ofFloat(0.0f), "BOOLEAN", false);
        assertCastFromVariant(Variant.ofFloat(-0.1f), "BOOLEAN", true);

        assertCastFromVariant(Variant.ofDouble(0.0), "BOOLEAN", false);
        assertCastFromVariant(Variant.ofDouble(123.456), "BOOLEAN", true);

        assertCastFromVariant(Variant.ofDecimal(new BigDecimal("0.0000")), "BOOLEAN", false);
        assertCastFromVariant(Variant.ofDecimal(new BigDecimal("0.0001")), "BOOLEAN", true);

        assertCastFromVariant(Variant.NULL_VALUE, "BOOLEAN", null);
    }

    @Test
    void testCastWithTinyint()
    {
        for (byte value : new byte[] {0, 1, -1, 127, -128, 42, -42}) {
            assertCastToVariant("TINYINT '%d'".formatted(value), value);
            assertCastFromVariant(Variant.ofByte(value), "TINYINT", value);
            assertCastFromVariant(Variant.ofString(String.valueOf(value)), "TINYINT", value);
        }

        assertCastFromVariant(Variant.ofBoolean(true), "TINYINT", (byte) 1);
        assertCastFromVariant(Variant.ofBoolean(false), "TINYINT", (byte) 0);

        assertCastFromVariant(Variant.ofShort((short) 42), "TINYINT", (byte) 42);
        assertCastFromVariantThrows(Variant.ofShort((short) (Byte.MAX_VALUE + 1)), "TINYINT")
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessageContaining("Out of range for tinyint: " + (Byte.MAX_VALUE + 1));

        assertCastFromVariant(Variant.ofInt(42), "TINYINT", (byte) 42);
        assertCastFromVariantThrows(Variant.ofInt(Byte.MAX_VALUE + 1), "TINYINT")
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessageContaining("Out of range for tinyint: " + (Byte.MAX_VALUE + 1));

        assertCastFromVariant(Variant.ofLong(42L), "TINYINT", (byte) 42);
        assertCastFromVariantThrows(Variant.ofLong(Byte.MAX_VALUE + 1L), "TINYINT")
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessageContaining("Out of range for tinyint: " + (Byte.MAX_VALUE + 1L));

        assertCastFromVariant(Variant.ofDecimal(new BigDecimal("42")), "TINYINT", (byte) 42);
        assertCastFromVariantThrows(Variant.ofDecimal(new BigDecimal("42.5")), "TINYINT")
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessageContaining("Out of range for tinyint: 42.5");
        assertCastFromVariantThrows(Variant.ofDecimal(new BigDecimal(Byte.MAX_VALUE + 1L)), "TINYINT")
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessageContaining("Out of range for tinyint: " + (Byte.MAX_VALUE + 1L));

        assertCastFromVariant(Variant.ofFloat(42.0f), "TINYINT", (byte) 42);
        assertCastFromVariant(Variant.ofDouble(42.0d), "TINYINT", (byte) 42);

        assertCastFromVariant(Variant.NULL_VALUE, "TINYINT", null);
    }

    @Test
    void testCastWithSmallint()
    {
        for (short value : new short[] {0, 1, -1, Short.MAX_VALUE, Short.MIN_VALUE, 42, -42}) {
            assertCastToVariant("SMALLINT '%d'".formatted(value), value);
            assertCastFromVariant(Variant.ofShort(value), "SMALLINT", value);
            assertCastFromVariant(Variant.ofString(String.valueOf(value)), "SMALLINT", value);
        }

        assertCastFromVariant(Variant.ofBoolean(true), "SMALLINT", (short) 1);
        assertCastFromVariant(Variant.ofBoolean(false), "SMALLINT", (short) 0);

        assertCastFromVariant(Variant.ofByte((byte) 123), "SMALLINT", (short) 123);

        assertCastFromVariant(Variant.ofInt(12345), "SMALLINT", (short) 12345);
        assertCastFromVariantThrows(Variant.ofInt(Short.MAX_VALUE + 1), "SMALLINT")
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessageContaining("Out of range for smallint: " + (Short.MAX_VALUE + 1));

        assertCastFromVariant(Variant.ofLong(12345L), "SMALLINT", (short) 12345);
        assertCastFromVariantThrows(Variant.ofLong(Short.MAX_VALUE + 1), "SMALLINT")
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessageContaining("Out of range for smallint: " + (Short.MAX_VALUE + 1));

        assertCastFromVariant(Variant.ofDecimal(new BigDecimal("123")), "SMALLINT", (short) 123);
        assertCastFromVariantThrows(Variant.ofDecimal(new BigDecimal("123.5")), "SMALLINT")
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessageContaining("Out of range for smallint: 123.5");
        assertCastFromVariantThrows(Variant.ofDecimal(new BigDecimal(Short.MAX_VALUE + 1L)), "SMALLINT")
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessageContaining("Out of range for smallint: " + (Short.MAX_VALUE + 1L));

        assertCastFromVariant(Variant.ofFloat(123.9f), "SMALLINT", (short) 124);
        assertCastFromVariant(Variant.ofDouble(123.9d), "SMALLINT", (short) 124);

        assertCastFromVariant(Variant.NULL_VALUE, "SMALLINT", null);
    }

    @Test
    void testCastWithInteger()
    {
        for (int value : new int[] {0, 1, -1, Integer.MAX_VALUE, Integer.MIN_VALUE, 42, -42}) {
            assertCastToVariant("INTEGER '%d'".formatted(value), value);
            assertCastFromVariant(Variant.ofInt(value), "INTEGER", value);
            assertCastFromVariant(Variant.ofString(String.valueOf(value)), "INTEGER", value);
        }

        assertCastFromVariant(Variant.ofBoolean(true), "INTEGER", 1);
        assertCastFromVariant(Variant.ofBoolean(false), "INTEGER", 0);

        assertCastFromVariant(Variant.ofByte((byte) 123), "INTEGER", 123);
        assertCastFromVariant(Variant.ofShort((short) 12345), "INTEGER", 12345);

        assertCastFromVariant(Variant.ofLong(123456L), "INTEGER", 123456);
        assertCastFromVariantThrows(Variant.ofLong(Integer.MAX_VALUE + 1L), "INTEGER")
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessageContaining("Out of range for integer: " + (Integer.MAX_VALUE + 1L));

        assertCastFromVariant(Variant.ofDecimal(new BigDecimal("1234")), "INTEGER", 1234);
        assertCastFromVariantThrows(Variant.ofDecimal(new BigDecimal("1234.5")), "INTEGER")
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessageContaining("Out of range for integer: 1234.5");
        assertCastFromVariantThrows(Variant.ofDecimal(new BigDecimal(Integer.MAX_VALUE + 1L)), "INTEGER")
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessageContaining("Out of range for integer: " + (Integer.MAX_VALUE + 1L));

        assertCastFromVariant(Variant.ofFloat(1234.6f), "INTEGER", 1235);
        assertCastFromVariant(Variant.ofDouble(1234.6d), "INTEGER", 1235);

        assertCastFromVariant(Variant.NULL_VALUE, "INTEGER", null);
    }

    @Test
    void testCastWithBigint()
    {
        for (long value : new long[] {0L, 1L, -1L, Long.MAX_VALUE, Long.MIN_VALUE, 42L, -42L}) {
            assertCastToVariant("BIGINT '%d'".formatted(value), value);
            assertCastFromVariant(Variant.ofLong(value), "BIGINT", value);
            assertCastFromVariant(Variant.ofString(String.valueOf(value)), "BIGINT", value);
        }

        assertCastFromVariant(Variant.ofBoolean(true), "BIGINT", 1L);
        assertCastFromVariant(Variant.ofBoolean(false), "BIGINT", 0L);

        assertCastFromVariant(Variant.ofByte((byte) 0x12), "BIGINT", 0x12L);
        assertCastFromVariant(Variant.ofShort((short) 0x1234), "BIGINT", 0x1234L);
        assertCastFromVariant(Variant.ofInt(0x1234_5678), "BIGINT", 0x1234_5678L);

        assertCastFromVariant(Variant.ofFloat(1234.5678f), "BIGINT", 1235L);
        assertCastFromVariant(Variant.ofDouble(1234.5678d), "BIGINT", 1235L);
        assertCastFromVariant(Variant.ofDecimal(new BigDecimal("1234")), "BIGINT", 1234L);
        assertCastFromVariantThrows(Variant.ofDecimal(new BigDecimal("1234.5")), "BIGINT")
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessageContaining("Out of range for bigint: 1234.5");
        assertCastFromVariantThrows(Variant.ofDecimal(new BigDecimal(Long.MAX_VALUE).add(ONE)), "BIGINT")
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessageContaining("Out of range for bigint: " + new BigDecimal(Long.MAX_VALUE).add(ONE));

        assertCastFromVariant(Variant.NULL_VALUE, "BIGINT", null);
    }

    @Test
    void testCastWithReal()
    {
        for (float value : new float[] {0.0f, 1.0f, -1.0f, 1234.5f, -1234.5f}) {
            assertCastToVariant("REAL '%s'".formatted(value), value);
            assertCastFromVariant(Variant.ofFloat(value), "REAL", value);
        }

        assertCastFromVariant(Variant.ofBoolean(true), "REAL", 1.0f);
        assertCastFromVariant(Variant.ofBoolean(false), "REAL", 0.0f);

        assertCastFromVariant(Variant.ofByte((byte) 5), "REAL", 5.0f);
        assertCastFromVariant(Variant.ofShort((short) -7), "REAL", -7.0f);
        assertCastFromVariant(Variant.ofInt(123_456), "REAL", 123_456.0f);
        assertCastFromVariant(Variant.ofLong(1234L), "REAL", 1234.0f);

        assertCastFromVariant(Variant.ofDecimal(new BigDecimal("1234.5")), "REAL", 1234.5f);
        assertCastFromVariant(Variant.ofDouble(1.5d), "REAL", 1.5f);

        assertCastFromVariant(Variant.ofString("1.5"), "REAL", 1.5f);

        assertCastFromVariant(Variant.NULL_VALUE, "REAL", null);
    }

    @Test
    void testCastWithDouble()
    {
        for (double value : new double[] {0.0, 1.0, -1.0, 1234.5, -1234.5}) {
            assertCastToVariant("DOUBLE '%s'".formatted(value), value);
            assertCastFromVariant(Variant.ofDouble(value), "DOUBLE", value);
            assertCastFromVariant(Variant.ofString(String.valueOf(value)), "DOUBLE", value);
        }

        assertCastFromVariant(Variant.ofBoolean(true), "DOUBLE", 1.0);
        assertCastFromVariant(Variant.ofBoolean(false), "DOUBLE", 0.0);

        assertCastFromVariant(Variant.ofByte((byte) 5), "DOUBLE", 5.0);
        assertCastFromVariant(Variant.ofShort((short) -7), "DOUBLE", -7.0);
        assertCastFromVariant(Variant.ofInt(123_456), "DOUBLE", 123_456.0);
        assertCastFromVariant(Variant.ofLong(1234L), "DOUBLE", 1234.0);

        assertCastFromVariant(Variant.ofDecimal(new BigDecimal("1234.5")), "DOUBLE", 1234.5);
        assertCastFromVariant(Variant.ofFloat(1.5f), "DOUBLE", 1.5);

        assertCastFromVariant(Variant.NULL_VALUE, "DOUBLE", null);
    }

    @Test
    void testCastWithShortDecimal()
    {
        BigDecimal[] values = {
                new BigDecimal("0"),
                new BigDecimal("1.23"),
                new BigDecimal("-1.23"),
                new BigDecimal("12345678.90"),
                new BigDecimal("12345678.912345"),
        };

        for (BigDecimal value : values) {
            assertCastToVariant("DECIMAL '%s'".formatted(value), value);

            BigDecimal scaled = value.setScale(2, HALF_UP);
            assertCastFromVariant(Variant.ofDecimal(value), "DECIMAL(10,2)", new SqlDecimal(scaled.unscaledValue(), 10, 2));
        }

        assertCastFromVariant(Variant.ofBoolean(true), "DECIMAL(10,2)", new SqlDecimal(BigInteger.valueOf(100), 10, 2));
        assertCastFromVariant(Variant.ofBoolean(false), "DECIMAL(10,2)", new SqlDecimal(BigInteger.ZERO, 10, 2));

        assertCastFromVariant(Variant.ofByte((byte) 5), "DECIMAL(10,2)", new SqlDecimal(BigInteger.valueOf(500), 10, 2));
        assertCastFromVariant(Variant.ofShort((short) -7), "DECIMAL(10,2)", new SqlDecimal(BigInteger.valueOf(-700), 10, 2));
        assertCastFromVariant(Variant.ofInt(123_456), "DECIMAL(10,2)", new SqlDecimal(BigInteger.valueOf(12_345_600), 10, 2));
        assertCastFromVariant(Variant.ofLong(12_345_678L), "DECIMAL(10,2)", new SqlDecimal(BigInteger.valueOf(1_234_567_800L), 10, 2));

        assertCastFromVariant(Variant.ofFloat(1.5f), "DECIMAL(10,2)", new SqlDecimal(BigInteger.valueOf(150), 10, 2));
        assertCastFromVariant(Variant.ofDouble(1.5d), "DECIMAL(10,2)", new SqlDecimal(BigInteger.valueOf(150), 10, 2));

        assertCastFromVariant(Variant.ofString("1234.56"), "DECIMAL(10,2)", new SqlDecimal(BigInteger.valueOf(123_456), 10, 2));
        assertCastFromVariant(Variant.ofString("1234.5678"), "DECIMAL(10,2)", new SqlDecimal(BigInteger.valueOf(123_457), 10, 2));

        assertCastFromVariant(Variant.NULL_VALUE, "DECIMAL(10,2)", null);
    }

    @Test
    void testCastWithLongDecimal()
    {
        int precision = 20;
        int scale = 4;

        BigDecimal[] values = {
                new BigDecimal("0"),
                new BigDecimal("1.2345"),
                new BigDecimal("-1.2345"),
                new BigDecimal("1234567890123.4567"),
                new BigDecimal("123456789012345.6789"),
        };

        for (BigDecimal value : values) {
            assertCastToVariant("DECIMAL '%s'".formatted(value), value);

            BigDecimal scaled = value.setScale(scale, HALF_UP);
            assertCastFromVariant(Variant.ofDecimal(value), "DECIMAL(20,4)", new SqlDecimal(scaled.unscaledValue(), precision, scale));
        }

        assertCastFromVariant(Variant.ofBoolean(true), "DECIMAL(20,4)", new SqlDecimal(BigInteger.valueOf(10_000), precision, scale));
        assertCastFromVariant(Variant.ofBoolean(false), "DECIMAL(20,4)", new SqlDecimal(BigInteger.ZERO, precision, scale));

        assertCastFromVariant(Variant.ofLong(1234L), "DECIMAL(20,4)", new SqlDecimal(BigInteger.valueOf(12_340_000L), precision, scale));

        assertCastFromVariant(Variant.ofString("1234.5678"), "DECIMAL(20,4)", new SqlDecimal(BigInteger.valueOf(12_345_678L), precision, scale));
        assertCastFromVariant(Variant.ofString("1234.56789"), "DECIMAL(20,4)", new SqlDecimal(BigInteger.valueOf(12_345_679L), precision, scale));

        assertCastFromVariant(Variant.ofDouble(1.5d), "DECIMAL(20,4)", new SqlDecimal(BigInteger.valueOf(15_000), precision, scale));
        assertCastFromVariant(Variant.ofFloat(1.5f), "DECIMAL(20,4)", new SqlDecimal(BigInteger.valueOf(15_000), precision, scale));

        assertCastFromVariant(Variant.NULL_VALUE, "DECIMAL(20,4)", null);
    }

    @Test
    void testCastWithVarchar()
    {
        for (String value : new String[] {"", "hello", "a somewhat longer string 123", "特殊字符", "emoji 😊", "x".repeat(1000)}) {
            assertCastToVariant("VARCHAR '%s'".formatted(value), value);
            assertCastFromVariant(Variant.ofString(value), "VARCHAR", value);
        }

        assertCastFromVariant(Variant.ofBoolean(true), "VARCHAR", "true");
        assertCastFromVariant(Variant.ofBoolean(false), "VARCHAR", "false");

        assertCastFromVariant(Variant.ofByte((byte) 5), "VARCHAR", "5");
        assertCastFromVariant(Variant.ofShort((short) -7), "VARCHAR", "-7");
        assertCastFromVariant(Variant.ofInt(123_456), "VARCHAR", "123456");
        assertCastFromVariant(Variant.ofLong(1234L), "VARCHAR", "1234");

        BigDecimal decimal = new BigDecimal("1234.50");
        assertCastFromVariant(Variant.ofDecimal(decimal), "VARCHAR", "1234.50");

        assertCastFromVariant(Variant.ofFloat(1.5f), "VARCHAR", "1.5E0");
        assertCastFromVariant(Variant.ofDouble(1.5d), "VARCHAR", "1.5E0");

        LocalDate date = LocalDate.of(2024, 10, 24);
        assertCastFromVariant(Variant.ofDate(date), "VARCHAR", "2024-10-24");

        assertCastFromVariant(Variant.ofTimestampMicrosNtz(LocalDateTime.parse("2024-10-24T12:34:56.123456")), "VARCHAR", "2024-10-24 12:34:56.123456");

        assertCastFromVariant(Variant.ofTimestampMicrosUtc(Instant.parse("2024-10-24T12:34:56.123456Z")), "VARCHAR", "2024-10-24 12:34:56.123456 UTC");

        assertCastFromVariant(Variant.ofTimestampNanosNtz(LocalDateTime.parse("2024-10-24T12:34:56.123456789")), "VARCHAR", "2024-10-24 12:34:56.123456789");

        assertCastFromVariant(Variant.ofTimestampNanosUtc(Instant.parse("2024-10-24T12:34:56.123456789Z")), "VARCHAR", "2024-10-24 12:34:56.123456789 UTC");

        UUID uuid = UUID.fromString("123e4567-e89b-12d3-a456-426655440000");
        assertCastFromVariant(Variant.ofUuid(uuid), "VARCHAR", "123e4567-e89b-12d3-a456-426655440000");

        assertCastFromVariant(Variant.ofString("short"), "VARCHAR", "short");

        assertCastFromVariant(Variant.NULL_VALUE, "VARCHAR", null);
    }

    @Test
    void testCastWithVarbinary()
    {
        Slice data = Slices.wrappedBuffer(new byte[] {0x01, 0x02, 0x03});

        assertCastToVariant("CAST(X'010203' as VARBINARY)", data);

        assertCastFromVariant(Variant.ofBinary(data), "VARBINARY", new SqlVarbinary(data.getBytes()));

        assertCastFromVariant(Variant.NULL_VALUE, "VARBINARY", null);
    }

    @Test
    void testCastWithDate()
    {
        LocalDate date = LocalDate.of(2024, 10, 24);
        int days = (int) date.toEpochDay();

        assertCastToVariant("DATE '2024-10-24'", date);

        assertCastFromVariant(Variant.ofDate(date), "DATE", new SqlDate(days));

        long epochMicros = TimeUnit.DAYS.toMicros(days) + TimeUnit.HOURS.toMicros(10);
        assertCastFromVariant(Variant.ofTimestampMicrosNtz(epochMicros), "DATE", new SqlDate(days));
        assertCastFromVariant(Variant.ofTimestampMicrosUtc(epochMicros), "DATE", new SqlDate(days));

        long epochNanos = TimeUnit.DAYS.toNanos(days) + TimeUnit.HOURS.toNanos(10);
        assertCastFromVariant(Variant.ofTimestampNanosNtz(epochNanos), "DATE", new SqlDate(days));
        assertCastFromVariant(Variant.ofTimestampNanosUtc(epochNanos), "DATE", new SqlDate(days));

        assertCastFromVariant(Variant.ofString("2024-10-24"), "DATE", new SqlDate(days));

        assertCastFromVariant(Variant.NULL_VALUE, "DATE", null);
    }

    @Test
    void testCastWithTime()
    {
        LocalTime localTime = LocalTime.of(22, 23, 24);
        assertCastToVariant("TIME '22:23:24.123456'", localTime.withNano(123_456_000));
        assertCastToVariant("TIME '22:23:24.12345'", localTime.withNano(123_450_000));
        assertCastToVariant("TIME '22:23:24.1234'", localTime.withNano(123_400_000));
        assertCastToVariant("TIME '22:23:24.123'", localTime.withNano(123_000_000));
        assertCastToVariant("TIME '22:23:24.12'", localTime.withNano(120_000_000));
        assertCastToVariant("TIME '22:23:24.1'", localTime.withNano(100_000_000));
        assertCastToVariant("TIME '22:23:24'", localTime);

        long epochNanos = localTime.withNano(123_456_123).toNanoOfDay();
        long epochMicros = epochNanos / 1000;
        assertCastFromVariant(Variant.ofTimeMicrosNtz(epochMicros), "TIME(6)", SqlTime.newInstance(6, epochMicros * 1_000_000L));

        assertCastFromVariant(Variant.ofTimeMicrosNtz(epochMicros), "TIME(3)", SqlTime.newInstance(3, (epochMicros / 1_000L) * 1_000_000_000L));

        assertCastFromVariant(Variant.ofString("22:23:24.123456"), "TIME(6)", SqlTime.newInstance(6, epochMicros * 1_000_000L));

        LocalDateTime localDateTime = LocalDateTime.parse("2024-10-24T22:23:24.123456123");
        assertCastFromVariant(Variant.ofTimestampMicrosNtz(localDateTime), "TIME(6)", SqlTime.newInstance(6, epochMicros * 1_000_000L));
        assertCastFromVariant(Variant.ofTimestampNanosNtz(localDateTime), "TIME(6)", SqlTime.newInstance(6, epochMicros * 1_000_000L));
        assertCastFromVariant(Variant.ofTimestampNanosNtz(localDateTime), "TIME(9)", SqlTime.newInstance(9, epochNanos * 1_000L));
        assertCastFromVariant(Variant.ofTimestampNanosNtz(localDateTime), "TIME(12)", SqlTime.newInstance(12, epochNanos * 1_000L));

        Instant instant = Instant.parse("2024-10-24T22:23:24.123456123Z");
        assertCastFromVariant(Variant.ofTimestampMicrosUtc(instant), "TIME(6)", SqlTime.newInstance(6, epochMicros * 1_000_000L));
        assertCastFromVariant(Variant.ofTimestampNanosUtc(instant), "TIME(6)", SqlTime.newInstance(6, epochMicros * 1_000_000L));
        assertCastFromVariant(Variant.ofTimestampNanosUtc(instant), "TIME(9)", SqlTime.newInstance(9, epochNanos * 1_000L));
        assertCastFromVariant(Variant.ofTimestampNanosUtc(instant), "TIME(12)", SqlTime.newInstance(12, epochNanos * 1_000L));

        assertCastFromVariant(Variant.NULL_VALUE, "TIME(3)", null);
        assertCastFromVariant(Variant.NULL_VALUE, "TIME(6)", null);
    }

    @Test
    void testCastWithTimestampShort()
    {
        LocalDateTime localDateTime = LocalDateTime.of(2024, 10, 24, 12, 34, 56);
        assertCastToVariant("TIMESTAMP '2024-10-24 12:34:56.123456'", localDateTime.withNano(123_456_000));
        assertCastToVariant("TIMESTAMP '2024-10-24 12:34:56.12345'", localDateTime.withNano(123_450_000));
        assertCastToVariant("TIMESTAMP '2024-10-24 12:34:56.1234'", localDateTime.withNano(123_400_000));
        assertCastToVariant("TIMESTAMP '2024-10-24 12:34:56.123'", localDateTime.withNano(123_000_000));
        assertCastToVariant("TIMESTAMP '2024-10-24 12:34:56.12'", localDateTime.withNano(120_000_000));
        assertCastToVariant("TIMESTAMP '2024-10-24 12:34:56.1'", localDateTime.withNano(100_000_000));
        assertCastToVariant("TIMESTAMP '2024-10-24 12:34:56'", localDateTime);

        long epochMicros = Instant.parse("2024-10-24T12:34:56.123Z").toEpochMilli() * 1000 + 456;

        assertCastFromVariant(Variant.ofTimestampMicrosNtz(epochMicros), "TIMESTAMP(6)", SqlTimestamp.newInstance(6, epochMicros, 0));
        assertCastFromVariant(Variant.ofTimestampMicrosUtc(epochMicros), "TIMESTAMP(6)", SqlTimestamp.newInstance(6, epochMicros, 0));

        assertCastFromVariant(Variant.ofTimestampMicrosNtz(epochMicros), "TIMESTAMP(2)", SqlTimestamp.newInstance(2, (epochMicros / 10_000) * 10_000, 0));
        assertCastFromVariant(Variant.ofTimestampMicrosUtc(epochMicros), "TIMESTAMP(2)", SqlTimestamp.newInstance(2, (epochMicros / 10_000) * 10_000, 0));

        long epochNanos = Instant.parse("2024-10-24T12:34:56.123Z").toEpochMilli() * 1_000_000 + 456_431;

        assertCastFromVariant(Variant.ofTimestampNanosNtz(epochNanos), "TIMESTAMP(6)", SqlTimestamp.newInstance(6, epochMicros, 0));
        assertCastFromVariant(Variant.ofTimestampNanosUtc(epochNanos), "TIMESTAMP(6)", SqlTimestamp.newInstance(6, epochMicros, 0));

        assertCastFromVariant(Variant.ofTimestampNanosNtz(epochNanos), "TIMESTAMP(2)", SqlTimestamp.newInstance(2, (epochNanos / 10_000_000) * 10_000, 0));
        assertCastFromVariant(Variant.ofTimestampNanosUtc(epochNanos), "TIMESTAMP(2)", SqlTimestamp.newInstance(2, (epochNanos / 10_000_000) * 10_000, 0));

        assertCastFromVariant(Variant.ofString("2024-10-24 12:34:56.123456"), "TIMESTAMP(6)", SqlTimestamp.newInstance(6, epochMicros, 0));

        LocalDate date = LocalDate.of(2024, 10, 24);
        assertCastFromVariant(Variant.ofDate(date), "TIMESTAMP(6)", SqlTimestamp.newInstance(6, TimeUnit.DAYS.toMicros(date.toEpochDay()), 0));

        assertCastFromVariant(Variant.NULL_VALUE, "TIMESTAMP(3)", null);
        assertCastFromVariant(Variant.NULL_VALUE, "TIMESTAMP(6)", null);
    }

    @Test
    void testCastWithTimestampLong()
    {
        LocalDateTime localDateTime = LocalDateTime.of(2024, 10, 24, 12, 34, 56);
        assertCastToVariant("TIMESTAMP '2024-10-24 12:34:56.123456789'", localDateTime.withNano(123_456_789));
        assertCastToVariant("TIMESTAMP '2024-10-24 12:34:56.12345678'", localDateTime.withNano(123_456_780));
        assertCastToVariant("TIMESTAMP '2024-10-24 12:34:56.1234567'", localDateTime.withNano(123_456_700));

        long epochSeconds = Instant.parse("2024-10-24T12:34:56.123Z").getEpochSecond();
        long nanosOfSecond = 123_456_789L;
        long nanos = epochSeconds * 1_000_000_000L + nanosOfSecond;

        assertCastFromVariant(Variant.ofTimestampNanosNtz(nanos), "TIMESTAMP(9)", SqlTimestamp.fromSeconds(9, epochSeconds, nanosOfSecond));
        assertCastFromVariant(Variant.ofTimestampNanosUtc(nanos), "TIMESTAMP(9)", SqlTimestamp.fromSeconds(9, epochSeconds, nanosOfSecond));

        long roundedNanosOfSecondP7 = ((nanosOfSecond + 50L) / 100L) * 100L;
        assertCastFromVariant(Variant.ofTimestampNanosNtz(nanos), "TIMESTAMP(7)", SqlTimestamp.fromSeconds(7, epochSeconds, roundedNanosOfSecondP7));
        assertCastFromVariant(Variant.ofTimestampNanosUtc(nanos), "TIMESTAMP(7)", SqlTimestamp.fromSeconds(7, epochSeconds, roundedNanosOfSecondP7));

        long microsOfSecond = 123_456L;
        long epochMicros = epochSeconds * 1_000_000L + microsOfSecond;

        assertCastFromVariant(Variant.ofTimestampMicrosNtz(epochMicros), "TIMESTAMP(9)", SqlTimestamp.fromSeconds(9, epochSeconds, microsOfSecond * 1_000L));
        assertCastFromVariant(Variant.ofTimestampMicrosUtc(epochMicros), "TIMESTAMP(9)", SqlTimestamp.fromSeconds(9, epochSeconds, microsOfSecond * 1_000L));

        assertCastFromVariant(Variant.ofString("2024-10-24 12:34:56.123456789"), "TIMESTAMP(9)", SqlTimestamp.fromSeconds(9, epochSeconds, nanosOfSecond));

        LocalDate date = LocalDate.of(2024, 10, 24);
        long dateMicros = TimeUnit.DAYS.toMicros(date.toEpochDay());
        assertCastFromVariant(Variant.ofDate(date), "TIMESTAMP(9)", SqlTimestamp.newInstance(9, dateMicros, 0));

        assertCastFromVariant(Variant.NULL_VALUE, "TIMESTAMP(9)", null);
        assertCastFromVariant(Variant.NULL_VALUE, "TIMESTAMP(12)", null);
    }

    @Test
    void testCastWithTimestampWithTimeZoneShort()
    {
        long epochSecond = Instant.parse("2024-10-24T12:34:56Z").getEpochSecond();
        assertCastToVariant("TIMESTAMP '2024-10-24 12:34:56.123Z'", Instant.ofEpochSecond(epochSecond, 123_000_000));
        assertCastToVariant("TIMESTAMP '2024-10-24 12:34:56.12Z'", Instant.ofEpochSecond(epochSecond, 120_000_000));
        assertCastToVariant("TIMESTAMP '2024-10-24 12:34:56.1Z'", Instant.ofEpochSecond(epochSecond, 100_000_000));
        assertCastToVariant("TIMESTAMP '2024-10-24 12:34:56Z'", Instant.ofEpochSecond(epochSecond, 0));

        Instant instant = Instant.parse("2024-10-24T12:34:56.789Z");
        long epochMicros = instant.toEpochMilli() * 1_000L;

        assertCastToVariant("TIMESTAMP '2024-10-24 12:34:56.789 UTC'", instant);

        assertCastFromVariant(Variant.ofTimestampMicrosUtc(epochMicros), "TIMESTAMP(3) WITH TIME ZONE", SqlTimestampWithTimeZone.fromInstant(3, instant, UTC));

        assertCastFromVariant(Variant.ofTimestampMicrosNtz(epochMicros), "TIMESTAMP(3) WITH TIME ZONE", SqlTimestampWithTimeZone.fromInstant(3, instant, UTC));

        assertCastFromVariant(Variant.ofTimestampMicrosUtc(epochMicros), "TIMESTAMP(4) WITH TIME ZONE", SqlTimestampWithTimeZone.fromInstant(4, instant, UTC));

        LocalDate date = LocalDate.of(2024, 10, 24);
        Instant dateInstant = date.atStartOfDay(UTC).toInstant();
        assertCastFromVariant(Variant.ofDate(date), "TIMESTAMP(3) WITH TIME ZONE", SqlTimestampWithTimeZone.fromInstant(3, dateInstant, UTC));

        assertCastFromVariant(Variant.ofString("2024-10-24 12:34:56.789 UTC"), "TIMESTAMP(3) WITH TIME ZONE", SqlTimestampWithTimeZone.fromInstant(3, instant, UTC));

        assertThat(assertions.expression("cast(a as VARIANT)")
                .binding("a", "TIMESTAMP '2024-10-24 12:34:56.789 UTC'"))
                .asInstanceOf(type(Variant.class))
                .extracting(Variant::getTimestampMicros)
                .isEqualTo(epochMicros);

        assertCastFromVariant(Variant.NULL_VALUE, "TIMESTAMP(3) WITH TIME ZONE", null);
    }

    @Test
    void testCastWithTimestampWithTimeZoneLong()
    {
        long epochSecond = Instant.parse("2024-10-24T12:34:56Z").getEpochSecond();
        assertCastToVariant("TIMESTAMP '2024-10-24 12:34:56.123456789Z'", Instant.ofEpochSecond(epochSecond, 123_456_789));
        assertCastToVariant("TIMESTAMP '2024-10-24 12:34:56.12345678Z'", Instant.ofEpochSecond(epochSecond, 123_456_780));
        assertCastToVariant("TIMESTAMP '2024-10-24 12:34:56.1234567Z'", Instant.ofEpochSecond(epochSecond, 123_456_700));
        assertCastToVariant("TIMESTAMP '2024-10-24 12:34:56.123456Z'", Instant.ofEpochSecond(epochSecond, 123_456_000));
        assertCastToVariant("TIMESTAMP '2024-10-24 12:34:56.12345Z'", Instant.ofEpochSecond(epochSecond, 123_450_000));
        assertCastToVariant("TIMESTAMP '2024-10-24 12:34:56.1234Z'", Instant.ofEpochSecond(epochSecond, 123_400_000));

        Instant instant = Instant.parse("2024-10-24T12:34:56.123456789Z");
        long epochNanos = instant.toEpochMilli() * 1_000_000L + 456_789L;

        assertCastFromVariant(Variant.ofTimestampNanosUtc(epochNanos), "TIMESTAMP(9) WITH TIME ZONE", SqlTimestampWithTimeZone.fromInstant(9, instant, UTC));

        assertCastFromVariant(Variant.ofTimestampNanosNtz(epochNanos), "TIMESTAMP(9) WITH TIME ZONE", SqlTimestampWithTimeZone.fromInstant(9, instant, UTC));

        assertCastFromVariant(Variant.ofTimestampNanosUtc(epochNanos), "TIMESTAMP(7) WITH TIME ZONE", SqlTimestampWithTimeZone.fromInstant(7, instant, UTC));

        LocalDate date = LocalDate.of(2024, 10, 24);
        Instant dateInstant = date.atStartOfDay(UTC).toInstant();
        assertCastFromVariant(Variant.ofDate(date), "TIMESTAMP(9) WITH TIME ZONE", SqlTimestampWithTimeZone.fromInstant(9, dateInstant, UTC));

        assertCastFromVariant(Variant.ofString("2024-10-24 12:34:56.123456789 UTC"), "TIMESTAMP(9) WITH TIME ZONE", SqlTimestampWithTimeZone.fromInstant(9, instant, UTC));

        assertThat(assertions.expression("cast(a as VARIANT)")
                .binding("a", "TIMESTAMP '2024-10-24 12:34:56.123456789 UTC'"))
                .asInstanceOf(type(Variant.class))
                .extracting(Variant::getTimestampNanos)
                .isEqualTo(epochNanos);

        assertCastFromVariant(Variant.NULL_VALUE, "TIMESTAMP(9) WITH TIME ZONE", null);
    }

    @Test
    void testCastWithUuid()
    {
        UUID uuid = UUID.fromString("123e4567-e89b-12d3-a456-426655440000");

        assertCastToVariant("UUID '123e4567-e89b-12d3-a456-426655440000'", uuid);

        assertCastFromVariant(Variant.ofUuid(uuid), "UUID", uuid.toString());
        assertCastFromVariant(Variant.ofString(uuid.toString()), "UUID", uuid.toString());

        assertCastFromVariant(Variant.NULL_VALUE, "UUID", null);
    }

    @Test
    void testCastWithJson()
    {
        // STRING → JSON
        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", toVariantLiteral(Variant.ofString("hello"))))
                .isEqualTo("\"hello\"");
        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", toVariantLiteral(Variant.ofString("emoji 😊"))))
                .isEqualTo("\"emoji 😊\"");
        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", toVariantLiteral(Variant.ofString("中文字符"))))
                .isEqualTo("\"中文字符\"");

        // BOOLEAN → JSON
        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", toVariantLiteral(Variant.ofBoolean(true))))
                .isEqualTo("true");

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", toVariantLiteral(Variant.ofBoolean(false))))
                .isEqualTo("false");

        // TINYINT → JSON
        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", toVariantLiteral(Variant.ofByte((byte) 5))))
                .isEqualTo("5");

        // SMALLINT → JSON
        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", toVariantLiteral(Variant.ofShort((short) -7))))
                .isEqualTo("-7");

        // INTEGER → JSON
        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", toVariantLiteral(Variant.ofInt(123_456))))
                .isEqualTo("123456");

        // BIGINT → JSON
        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", toVariantLiteral(Variant.ofLong(1234L))))
                .isEqualTo("1234");

        // DECIMAL → JSON
        BigDecimal decimal = new BigDecimal("1234.50");
        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", toVariantLiteral(Variant.ofDecimal(decimal))))
                .isEqualTo("1234.50");

        // REAL → JSON
        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", toVariantLiteral(Variant.ofFloat(1.5f))))
                .isEqualTo("1.5");

        // DOUBLE → JSON
        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", toVariantLiteral(Variant.ofDouble(1.5d))))
                .isEqualTo("1.5");

        // DATE → JSON (string)
        LocalDate date = LocalDate.of(2024, 10, 24);
        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", toVariantLiteral(Variant.ofDate(date))))
                .isEqualTo("\"2024-10-24\"");

        // TIMESTAMP_MICROS_NTZ → JSON (string)
        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", toVariantLiteral(
                        Variant.ofTimestampMicrosNtz(LocalDateTime.parse("2024-10-24T12:34:56.123456")))))
                .isEqualTo("\"2024-10-24 12:34:56.123456\"");

        // TIMESTAMP_MICROS_UTC → JSON (string with zone)
        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", toVariantLiteral(
                        Variant.ofTimestampMicrosUtc(Instant.parse("2024-10-24T12:34:56.123456Z")))))
                .isEqualTo("\"2024-10-24 12:34:56.123456 UTC\"");

        // TIMESTAMP_NANOS_NTZ → JSON (string)
        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", toVariantLiteral(
                        Variant.ofTimestampNanosNtz(LocalDateTime.parse("2024-10-24T12:34:56.123456789")))))
                .isEqualTo("\"2024-10-24 12:34:56.123456789\"");

        // TIMESTAMP_NANOS_UTC → JSON (string with zone)
        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", toVariantLiteral(
                        Variant.ofTimestampNanosUtc(Instant.parse("2024-10-24T12:34:56.123456789Z")))))
                .isEqualTo("\"2024-10-24 12:34:56.123456789 UTC\"");

        // UUID → JSON (string)
        UUID uuid = UUID.fromString("123e4567-e89b-12d3-a456-426655440000");
        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", toVariantLiteral(Variant.ofUuid(uuid))))
                .isEqualTo("\"123e4567-e89b-12d3-a456-426655440000\"");

        // BINARY → JSON (base64 string of "abc" → "YWJj")
        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", toVariantLiteral(Variant.ofBinary(utf8Slice("abc")))))
                .isEqualTo("\"YWJj\"");

        // ARRAY → JSON
        // Adjust to your actual array-construction helper if different
        Variant arrayVariant = Variant.ofArray(List.of(Variant.ofInt(1), Variant.ofString("two")));
        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", toVariantLiteral(arrayVariant)))
                .isEqualTo("[1,\"two\"]");

        // OBJECT → JSON
        // Adjust to your actual object-construction helper if different
        Variant objectVariant = Variant.ofObject(Map.of(
                utf8Slice("a"), Variant.ofInt(1),
                utf8Slice("b"), Variant.ofString("two")));
        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", toVariantLiteral(objectVariant)))
                .isEqualTo("{\"a\":1,\"b\":\"two\"}");

        // VARIANT primitive NULL → JSON 'null' (as a JSON value, not SQL NULL)
        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", toVariantLiteral(Variant.NULL_VALUE)))
                .isEqualTo("null");
    }

    @Test
    void testCastJsonPrimitivesToVariant()
    {
        assertThat(assertions.expression("CAST(a AS VARIANT)")
                .binding("a", "JSON 'null'"))
                .asInstanceOf(type(Variant.class))
                .extracting(Variant::isNull)
                .isEqualTo(true);

        assertCastToVariant("JSON 'true'", true);
        assertCastToVariant("JSON 'false'", false);

        assertCastToVariant("JSON '123'", 123);
        assertCastToVariant("JSON '1234567890123'", 1234567890123L);

        BigDecimal bigDecimal = new BigDecimal("12345678901234567890123456789012345678");
        assertCastToVariant("JSON '%s'".formatted(bigDecimal), bigDecimal);

        assertCastToVariant("JSON '1234.50'", 1234.5);

        assertCastToVariant("JSON '\"hello\"'", "hello");
        assertCastToVariant("JSON '\"emoji 😊\"'", "emoji 😊");

        assertCastToVariant("JSON '[]'", List.of());
        assertCastToVariant("JSON '[1, 2, 3]'", List.of(1, 2, 3));
        assertCastToVariant("JSON '{}'", Map.of());
        assertCastToVariant("JSON '{\"a\": 1, \"b\": \"two\"}'", Map.of("a", 1, "b", "two"));
    }

    @Test
    void testCastJsonArraysAndObjectsRoundTrip()
    {
        // Simple array
        assertThat(assertions.expression("CAST(CAST(a AS VARIANT) AS JSON)")
                .binding("a", "JSON '[1, 2, 3]'"))
                .isEqualTo("[1,2,3]");

        // Nested arrays and objects
        assertThat(assertions.expression("CAST(CAST(a AS VARIANT) AS JSON)")
                .binding("a", "JSON '{\"a\": [1, {\"b\": true}], \"c\": null}'"))
                .isEqualTo("{\"a\":[1,{\"b\":true}],\"c\":null}");

        // Empty array
        assertThat(assertions.expression("CAST(CAST(a AS VARIANT) AS JSON)")
                .binding("a", "JSON '[]'"))
                .isEqualTo("[]");

        // Empty object
        assertThat(assertions.expression("CAST(CAST(a AS VARIANT) AS JSON)")
                .binding("a", "JSON '{}'"))
                .isEqualTo("{}");
    }

    @Test
    void testCastJsonToVariantMetadataAndFieldOrdering()
    {
        // Top-level array of objects with different field sets and order
        // This exercises:
        //  * global metadata over the whole JSON tree
        //  * object-field encoding sorted by UTF-8 name (via Slice sort)
        //  * correct field-id assignment per object
        String json = """
                [
                  {"b": 1, "a": 2},
                  {"c": 3, "a": 4}
                ]
                """;

        assertThat(assertions.expression("CAST(a AS VARIANT)")
                .binding("a", "JSON '%s'".formatted(json)))
                .asInstanceOf(type(Variant.class))
                .satisfies(variant -> {
                    assertThat(variant.metadata().isSorted()).isTrue();
                    // Root is an array
                    List<Variant> elements = variant.arrayElements().toList();
                    assertThat(elements).hasSize(2);

                    Metadata metadata = variant.metadata();

                    int idA = metadata.id(utf8Slice("a"));
                    int idB = metadata.id(utf8Slice("b"));
                    int idC = metadata.id(utf8Slice("c"));

                    // First object: {"b":1,"a":2}
                    Variant object1 = elements.getFirst();
                    // verify fields are written in sorted order by field name UTF-8 bytes
                    assertThat(object1.objectFields().toList())
                            .extracting(ObjectFieldIdValue::fieldId)
                            .containsExactly(idA, idB);

                    assertThat(object1.getObjectField(idA).orElseThrow().getInt()).isEqualTo(2);
                    assertThat(object1.getObjectField(idB).orElseThrow().getInt()).isEqualTo(1);

                    // Second object: {"c":3,"a":4}
                    Variant object2 = elements.get(1);
                    // verify fields are written in sorted order by field name UTF-8 bytes
                    assertThat(object2.objectFields().toList())
                            .extracting(ObjectFieldIdValue::fieldId)
                            .containsExactly(idA, idC);

                    assertThat(object2.getObjectField(idA).orElseThrow().getInt()).isEqualTo(4);
                    assertThat(object2.getObjectField(idC).orElseThrow().getInt()).isEqualTo(3);
                });
    }

    @Test
    void testCastJsonToVariantUtf8FieldOrdering()
    {
        // Use some non-ASCII field names to exercise the Slice/UTF-8 sort.
        // These are chosen just to make sure we're not assuming ASCII-only.
        String json = """
                {
                  "é": 1,
                  "e": 2,
                  "Ω": 3
                }
                """;

        assertThat(assertions.expression("CAST(a AS VARIANT)")
                .binding("a", "JSON '%s'".formatted(json)))
                .asInstanceOf(type(Variant.class))
                .satisfies(variant -> {
                    Metadata metadata = variant.metadata();
                    assertThat(metadata.isSorted()).isTrue();

                    // Binary UTF-8 ordering: "e" < "é" < "Ω"
                    assertThat(metadata.get(0)).isEqualTo(utf8Slice("e"));
                    assertThat(metadata.get(1)).isEqualTo(utf8Slice("é"));
                    assertThat(metadata.get(2)).isEqualTo(utf8Slice("Ω"));

                    // Object fields should reference those metadata entries
                    assertThat(variant.objectFields()
                            .map(ObjectFieldIdValue::fieldId)
                            .map(metadata::get)
                            .map(Slice::toStringUtf8)
                            .toList())
                            .containsExactly("e", "é", "Ω");
                });

        // Also round-trip JSON -> VARIANT -> JSON structurally
        assertThat(assertions.expression("CAST(CAST(a AS VARIANT) AS JSON)")
                .binding("a", "JSON '%s'".formatted(json.replace("'", "''"))))
                .isEqualTo("{\"e\":2,\"é\":1,\"Ω\":3}");
    }

    @Test
    void testVariantToArrayCast()
    {
        Variant arrayVariant = Variant.ofArray(Arrays.asList(
                Variant.ofBoolean(true),
                Variant.ofByte((byte) 10),
                Variant.ofShort((byte) 20),
                Variant.ofInt(30),
                Variant.ofLong(40),
                Variant.ofFloat(50),
                Variant.ofDouble(60),
                Variant.ofString("70")));

        // VARIANT -> ARRAY<INTEGER>
        assertThat(assertions.expression("cast(a as ARRAY<INTEGER>)")
                .binding("a", toVariantLiteral(arrayVariant)))
                .asInstanceOf(list(Integer.class))
                .containsExactly(1, 10, 20, 30, 40, 50, 60, 70);

        // VARIANT -> ARRAY<TINYINT>
        assertThat(assertions.expression("cast(a as ARRAY<TINYINT>)")
                .binding("a", toVariantLiteral(arrayVariant)))
                .asInstanceOf(list(Byte.class))
                .containsExactly((byte) 1, (byte) 10, (byte) 20, (byte) 30, (byte) 40, (byte) 50, (byte) 60, (byte) 70);

        // VARIANT -> ARRAY<VARIANT>
        assertThat(assertions.expression("cast(a as ARRAY<VARIANT>)")
                .binding("a", toVariantLiteral(arrayVariant)))
                .asInstanceOf(list(Variant.class))
                .satisfies(list -> {
                    assertThat(list).hasSize(8);
                    assertThat((Variant) list.get(0)).extracting(Variant::getBoolean).isEqualTo(true);
                    assertThat((Variant) list.get(3)).extracting(Variant::getInt).isEqualTo(30);
                    assertThat((Variant) list.get(7)).extracting(variant -> variant.getString().toStringUtf8()).isEqualTo("70");
                });
    }

    @Test
    void testArrayToVariantCast()
    {
        List<Integer> intElements = Arrays.asList(1, 10, 20, 30, 40, 50, 60, 70);
        String intArrayLiteral = intElements.stream()
                .map(String::valueOf)
                .collect(Collectors.joining(", ", "ARRAY[", "]"));
        assertCastToVariant(intArrayLiteral, intElements);

        List<String> stringElements = Arrays.asList("one", "two", "three");
        String stringArrayLiteral = stringElements.stream()
                .map(value -> "'" + value + "'")
                .collect(Collectors.joining(", ", "ARRAY[", "]"));
        assertCastToVariant(stringArrayLiteral, stringElements);
    }

    @Test
    void testMapToVariantCast()
    {
        // Basic map cast (string keys, primitive values)
        // Variant preserves key case
        assertCastToVariant(
                "MAP(ARRAY['banAna', 'appLE', 'chERry'], ARRAY[2, 1, 3])",
                Map.of(
                        "appLE", 1,
                        "banAna", 2,
                        "chERry", 3));

        // Nested map values (generic map writer path)
        assertCastToVariant(
                "MAP(ARRAY['b', 'a'], ARRAY[MAP(ARRAY['y'], ARRAY[10]), MAP(ARRAY['x'], ARRAY[20])])",
                Map.of(
                        "a", Map.of("x", 20),
                        "b", Map.of("y", 10)));

        // Map values that are arrays (generic map writer path)
        assertCastToVariant(
                "MAP(ARRAY['b', 'a'], ARRAY[ARRAY[1, 2, 3], ARRAY[4, 5]])",
                Map.of(
                        "a", List.of(4, 5),
                        "b", List.of(1, 2, 3)));

        // Note, duplicate keys and null keys cannot be tested easily because Trino MAP constructor
        // itself enforces non-null unique keys. This could be done with a custom function,
        // but is not worth the effort.
    }

    @Test
    void testRowToVariantCast()
    {
        // Trino fields are always in upper-case and this is preserved in Variant
        assertCastToVariant(
                "ROW(1 AS a, 'two' AS b, TRUE AS c)",
                Map.of(
                        "A", 1,
                        "B", "two",
                        "C", true));

        assertCastToVariant(
                "ROW('two' AS b, 1 AS a, TRUE AS c)",
                Map.of(
                        "A", 1,
                        "B", "two",
                        "C", true));

        String oneOfEverythingRowLiteral = """
                ROW(
                  NULL AS a,
                  TRUE AS b,
                  123 AS c,
                  1234567890123 AS d,
                  1234.56 AS e,
                  REAL '1.5' AS f,
                  'hello' AS g,
                  DATE '2024-10-24' AS h,
                  TIMESTAMP '2024-10-24 12:34:56.123456' AS i,
                  UUID '123e4567-e89b-12d3-a456-426655440000' AS j,
                  ARRAY[1, 2, 3] AS k,
                  ROW('y' as x, 10 as z) AS l
                )
                """;
        Map<String, Object> expectedMap = new LinkedHashMap<>();
        expectedMap.put("A", null);
        expectedMap.put("B", true);
        expectedMap.put("C", 123);
        expectedMap.put("D", 1234567890123L);
        expectedMap.put("E", new BigDecimal("1234.56"));
        expectedMap.put("F", 1.5f);
        expectedMap.put("G", "hello");
        expectedMap.put("H", LocalDate.of(2024, 10, 24));
        expectedMap.put("I", LocalDateTime.of(2024, 10, 24, 12, 34, 56, 123456000));
        expectedMap.put("J", UUID.fromString("123e4567-e89b-12d3-a456-426655440000"));
        expectedMap.put("K", List.of(1, 2, 3));
        expectedMap.put("L", Map.of(
                "X", "y",
                "Z", 10));

        assertCastToVariant(oneOfEverythingRowLiteral, expectedMap);
    }

    @Test
    void testVariantLeafNoMetadataIsCopied()
    {
        Variant leaf = Variant.ofInt(123);

        assertThat(assertions.expression("cast(a as VARIANT)")
                .binding("a", "ARRAY[%s, %s]".formatted(
                        toVariantLiteral(leaf),
                        toVariantLiteral(leaf))))
                .asInstanceOf(type(Variant.class))
                .satisfies(variant -> {
                    assertThat(variant.metadata().dictionarySize()).isEqualTo(0);
                    assertThat(variant.arrayElements()
                            .map(Variant::getInt)
                            .toList())
                            .isEqualTo(List.of(123, 123));
                });
    }

    @Test
    void testVariantLeafSameSizeRemap()
    {
        Variant leaf1 = Variant.ofObject(Map.of(
                utf8Slice("b"), Variant.ofInt(1),
                utf8Slice("a"), Variant.ofString("x")));

        Variant leaf2 = Variant.ofObject(Map.of(
                utf8Slice("c"), Variant.ofInt(2)));

        assertThat(assertions.expression("cast(a as VARIANT)")
                .binding("a", "ARRAY[%s, %s]".formatted(
                        toVariantLiteral(leaf1),
                        toVariantLiteral(leaf2))))
                .asInstanceOf(type(Variant.class))
                .satisfies(variant -> {
                    assertThat(variant.metadata().dictionarySize()).isEqualTo(3);
                    assertThat(variant.metadata().isSorted()).isTrue();

                    assertThat(variant.arrayElements()
                            .map(Variant::toObject)
                            .toList())
                            .isEqualTo(List.of(
                                    Map.of("a", "x", "b", 1),
                                    Map.of("c", 2)));
                });
    }

    @Test
    void testVariantLeafResizeRemap()
    {
        Variant big = objectVariantWithManyFields(260, "a");

        Variant small = Variant.ofObject(Map.of(
                utf8Slice("z"), Variant.ofInt(7)));

        assertThat(assertions.expression("cast(a as VARIANT)")
                .binding("a", "ARRAY[%s, %s]".formatted(
                        toVariantLiteral(big),
                        toVariantLiteral(small))))
                .asInstanceOf(type(Variant.class))
                .satisfies(variant -> {
                    assertThat(variant.metadata().dictionarySize()).isGreaterThan(256);
                    assertThat(variant.metadata().isSorted()).isTrue();

                    List<Variant> elements = variant.arrayElements().toList();
                    assertThat(elements).hasSize(2);
                    assertThat(elements.get(1).toObject()).isEqualTo(Map.of("z", 7));
                });
    }

    @Test
    void testVariantLeafDeepRecursionRemap()
    {
        Variant leaf = Variant.ofObject(ImmutableSortedMap.of(
                utf8Slice("outer"),
                Variant.ofObject(Map.of(
                        utf8Slice("inner"),
                        Variant.ofArray(List.of(
                                Variant.ofObject(Map.of(utf8Slice("x"), Variant.ofInt(10))),
                                Variant.ofObject(Map.of(utf8Slice("y"), Variant.ofInt(20)))))))));

        Variant big = objectVariantWithManyFields(300, "a");

        assertThat(assertions.expression("cast(a as VARIANT)")
                .binding("a", "ARRAY[%s, %s]".formatted(
                        toVariantLiteral(big),
                        toVariantLiteral(leaf))))
                .asInstanceOf(type(Variant.class))
                .satisfies(variant -> {
                    assertThat(variant.metadata().dictionarySize()).isGreaterThan(256);
                    assertThat(variant.metadata().isSorted()).isTrue();

                    List<Variant> elements = variant.arrayElements().toList();
                    assertThat(elements).hasSize(2);
                    assertThat(elements.get(1).toObject())
                            .isEqualTo(Map.of(
                                    "outer", Map.of(
                                            "inner", List.of(
                                                    Map.of("x", 10),
                                                    Map.of("y", 20)))));
                });
    }

    @Test
    void testVariantLeafArraySameSizeRemapCopiesArrayHeader()
    {
        // Big object contributes lots of metadata entries, but stays < 256 so field-id width stays 1 byte.
        Variant big = objectVariantWithManyFields(200, "a");

        // Leaf is an ARRAY whose elements are OBJECTs with unsorted metadata ("b", "a").
        // The outer cast will merge metadata (including big's names) and remap these field ids,
        // but the remap stays SAME_SIZE (1-byte ids).
        Metadata leafMetadata = Metadata.of(List.of(utf8Slice("b"), utf8Slice("a")));
        // TODO this must be built by hand to prevent automatic sorting
        Variant leafArray = Variant.ofArray(List.of(
                createObjectWithSortedFields(leafMetadata, List.of(
                        new ObjectField(0, Variant.ofInt(1)),
                        new ObjectField(1, Variant.ofString("x")))),
                createObjectWithSortedFields(leafMetadata, List.of(
                        new ObjectField(0, Variant.ofInt(2)),
                        new ObjectField(1, Variant.ofString("y"))))));

        assertThat(assertions.expression("cast(a as VARIANT)")
                .binding("a", "ARRAY[%s, %s]".formatted(
                        toVariantLiteral(big),
                        toVariantLiteral(leafArray))))
                .asInstanceOf(type(Variant.class))
                .satisfies(variant -> {
                    // Ensure we stayed in SAME_SIZE territory (unsigned byte ids)
                    assertThat(variant.metadata().dictionarySize()).isLessThanOrEqualTo(256);
                    assertThat(variant.metadata().isSorted()).isTrue();

                    List<Variant> elements = variant.arrayElements().toList();
                    assertThat(elements).hasSize(2);

                    // Second element is the leaf ARRAY; verify semantics survived remap.
                    assertThat(elements.get(1).toObject())
                            .isEqualTo(List.of(
                                    Map.of("a", "x", "b", 1),
                                    Map.of("a", "y", "b", 2)));
                });
    }
    private static Variant objectVariantWithManyFields(int fieldCount, String prefix)
    {
        List<Slice> names = new ArrayList<>(fieldCount);
        List<ObjectField> fields = new ArrayList<>(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            // fixed width keeps lexicographic ordering predictable
            Slice name = utf8Slice("%s%03d".formatted(prefix, i));
            names.add(name);
            fields.add(new ObjectField(i, Variant.ofInt(i)));
        }
        return createObjectWithSortedFields(Metadata.of(names), fields);
    }

    @Test
    void testArrayToVariantCastWithVariantElements()
    {
        Variant leaf1 = createObjectWithSortedFields(
                Metadata.of(List.of(utf8Slice("b"), utf8Slice("a"))),
                List.of(
                        new ObjectField(0, Variant.ofInt(1)),
                        new ObjectField(1, Variant.ofString("x"))));

        Variant leaf2 = createObjectWithSortedFields(
                Metadata.of(List.of(utf8Slice("c"), utf8Slice("a"))),
                List.of(
                        new ObjectField(0, Variant.ofInt(2)),
                        new ObjectField(1, Variant.ofInt(3))));

        assertCastToVariant(
                "ARRAY[%s, %s]".formatted(toVariantLiteral(leaf1), toVariantLiteral(leaf2)),
                List.of(
                        Map.of("a", "x", "b", 1),
                        Map.of("a", 3, "c", 2))
        );
    }

    @Test
    void testMapToVariantCastWithVariantValues()
    {
        Variant leaf1 = createObjectWithSortedFields(
                Metadata.of(List.of(utf8Slice("b"), utf8Slice("a"))),
                List.of(
                        new ObjectField(0, Variant.ofInt(1)),
                        new ObjectField(1, Variant.ofString("x"))));

        Variant leaf2 = createObjectWithSortedFields(
                Metadata.of(List.of(utf8Slice("c"), utf8Slice("a"))),
                List.of(
                        new ObjectField(0, Variant.ofInt(2)),
                        new ObjectField(1, Variant.ofInt(3))));

        assertCastToVariant(
                "MAP(ARRAY['k2', 'k1'], ARRAY[%s, %s])".formatted(toVariantLiteral(leaf2), toVariantLiteral(leaf1)),
                Map.of(
                        "k1", Map.of("a", "x", "b", 1),
                        "k2", Map.of("a", 3, "c", 2)));
    }

    @Test
    void testRowToVariantCastWithVariantFields()
    {
        Variant leaf1 = createObjectWithSortedFields(
                Metadata.of(List.of(utf8Slice("b"), utf8Slice("a"))),
                List.of(
                        new ObjectField(0, Variant.ofInt(1)),
                        new ObjectField(1, Variant.ofString("x"))));

        Variant leaf2 = createObjectWithSortedFields(
                Metadata.of(List.of(utf8Slice("c"), utf8Slice("a"))),
                List.of(
                        new ObjectField(0, Variant.ofInt(2)),
                        new ObjectField(1, Variant.ofInt(3))));

        assertCastToVariant(
                "ROW(%s AS v1, 10 AS x, %s AS v2)".formatted(toVariantLiteral(leaf1), toVariantLiteral(leaf2)),
                Map.of(
                        "V1", Map.of("a", "x", "b", 1),
                        "X", 10,
                        "V2", Map.of("a", 3, "c", 2)));
    }

    @Test
    void testArrayDereference()
    {
        Variant arrayVariant = Variant.fromObject(List.of(10, true, "😊"));

        assertThat(assertions.expression("a[1]")
                .binding("a", toVariantLiteral(arrayVariant)))
                .asInstanceOf(type(Variant.class))
                .extracting(Variant::getInt)
                .isEqualTo(10);
        assertThat(assertions.expression("a[3]")
                .binding("a", toVariantLiteral(arrayVariant)))
                .asInstanceOf(type(Variant.class))
                .extracting(variant -> variant.getString().toStringUtf8())
                .isEqualTo("😊");

        // out of bounds is an error
        assertTrinoExceptionThrownBy(() -> assertions.expression("a[0]")
                .binding("a", toVariantLiteral(arrayVariant))
                .evaluate())
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessage("VARIANT array indices start at 1");
        assertTrinoExceptionThrownBy(() -> assertions.expression("a[4]")
                .binding("a", toVariantLiteral(arrayVariant))
                .evaluate())
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessage("VARIANT array subscript must be less than or equal to array length: 4 > 3");
        assertTrinoExceptionThrownBy(() -> assertions.expression("a[-4]")
                .binding("a", toVariantLiteral(arrayVariant))
                .evaluate())
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessage("VARIANT array subscript is negative: -4");
    }

    @Test
    void testObjectDereference()
    {
        Variant arrayVariant = createObjectWithSortedFields(
                Metadata.of(List.of(utf8Slice("apple"), utf8Slice("BANANA"), utf8Slice("c"))),
                List.of(
                        new ObjectField(1, Variant.ofInt(10)),
                        new ObjectField(2, Variant.ofBoolean(true)),
                        new ObjectField(0, Variant.ofString("😊"))));

        assertThat(assertions.expression("a['apple']")
                .binding("a", toVariantLiteral(arrayVariant)))
                .asInstanceOf(type(Variant.class))
                .extracting(variant -> variant.getString().toStringUtf8())
                .isEqualTo("😊");
        assertThat(assertions.expression("a['BANANA']")
                .binding("a", toVariantLiteral(arrayVariant)))
                .asInstanceOf(type(Variant.class))
                .extracting(Variant::getInt)
                .isEqualTo(10);
        assertThat(assertions.expression("a['c']")
                .binding("a", toVariantLiteral(arrayVariant)))
                .asInstanceOf(type(Variant.class))
                .extracting(Variant::getBoolean)
                .isEqualTo(true);

        // unknown key returns null
        assertThat(assertions.expression("a['unknown']")
                .binding("a", toVariantLiteral(arrayVariant)))
                .isNull();

        // keys are case sensitive
        assertThat(assertions.expression("a['Apple']")
                .binding("a", toVariantLiteral(arrayVariant)))
                .isNull();
        assertThat(assertions.expression("a['banana']")
                .binding("a", toVariantLiteral(arrayVariant)))
                .isNull();
    }

    private void assertCastToVariant(String sqlLiteral, Object expected)
    {
        assertThat(assertions.expression("CAST(a as VARIANT)")
                .binding("a", sqlLiteral))
                .asInstanceOf(type(Variant.class))
                .extracting(Variant::toObject)
                .isEqualTo(expected);

        assertThat(assertions.expression("CAST(a AS VARIANT)")
                .binding("a", "ARRAY[%s, %s, %s]".formatted(sqlLiteral, sqlLiteral, sqlLiteral)))
                .asInstanceOf(type(Variant.class))
                .extracting(Variant::toObject)
                .isEqualTo(Arrays.asList(expected, expected, expected));

        assertThat(assertions.expression("CAST(a AS VARIANT)")
                .binding("a", "MAP(ARRAY['key1', 'key2', 'key3'], ARRAY[%s, %s, %s])".formatted(sqlLiteral, sqlLiteral, sqlLiteral)))
                .asInstanceOf(type(Variant.class))
                .extracting(Variant::toObject)
                .isEqualTo(Map.of(
                        "key1", expected,
                        "key2", expected,
                        "key3", expected));

        assertThat(assertions.expression("CAST(a AS VARIANT)")
                .binding("a", "ROW(%s AS col1, %s AS col2, %s AS col3)".formatted(sqlLiteral, sqlLiteral, sqlLiteral)))
                .asInstanceOf(type(Variant.class))
                .extracting(Variant::toObject)
                .isEqualTo(Map.of(
                        "COL1", expected,
                        "COL2", expected,
                        "COL3", expected));

        assertThat(assertions.expression("CAST(a as VARIANT) = CAST(b as VARIANT)")
                .binding("a", sqlLiteral)
                .binding("b", sqlLiteral))
                .isEqualTo(true);

        assertThat(assertions.expression("CAST(a as VARIANT) = CAST(b as VARIANT)")
                .binding("a", "ARRAY[%s, %s, %s]".formatted(sqlLiteral, sqlLiteral, sqlLiteral))
                .binding("b", "ARRAY[%s, %s, %s]".formatted(sqlLiteral, sqlLiteral, sqlLiteral)))
                .isEqualTo(true);

        assertThat(assertions.expression("CAST(a as VARIANT) = CAST(b as VARIANT)")
                .binding("a", "MAP(ARRAY['key1', 'key2', 'key3'], ARRAY[%s, %s, %s])".formatted(sqlLiteral, sqlLiteral, sqlLiteral))
                .binding("b", "MAP(ARRAY['key1', 'key2', 'key3'], ARRAY[%s, %s, %s])".formatted(sqlLiteral, sqlLiteral, sqlLiteral)))
                .isEqualTo(true);

        assertThat(assertions.expression("CAST(a as VARIANT) = CAST(b as VARIANT)")
                .binding("a", "ROW(%s AS col1, %s AS col2, %s AS col3)".formatted(sqlLiteral, sqlLiteral, sqlLiteral))
                .binding("b", "ROW(%s AS col1, %s AS col2, %s AS col3)".formatted(sqlLiteral, sqlLiteral, sqlLiteral)))
                .isEqualTo(true);
    }

    private void assertCastFromVariant(Variant variant, String type, Object value)
    {
        assertThat(assertions.expression("CAST(a as %s)".formatted(type))
                .binding("a", toVariantLiteral(variant)))
                .isEqualTo(value);

        assertThat(assertions.expression("CAST(a AS ARRAY<%s>)".formatted(type))
                .binding("a", toVariantLiteral(Variant.ofArray(Arrays.asList(variant, variant, variant)))))
                .asInstanceOf(list(Object.class))
                .containsExactly(value, value, value);

        assertThat(assertions.expression("CAST(a AS MAP<VARCHAR, %s>)".formatted(type))
                .binding(
                        "a", toVariantLiteral(createObjectWithSortedFields(
                                Metadata.of(List.of(utf8Slice("key1"), utf8Slice("key2"), utf8Slice("key3"))),
                                List.of(
                                        new ObjectField(0, variant),
                                        new ObjectField(1, variant),
                                        new ObjectField(2, variant))))))
                .asInstanceOf(map(String.class, Object.class))
                .containsExactly(
                        entry("key1", value),
                        entry("key2", value),
                        entry("key3", value));

        assertThat(assertions.expression("CAST(a AS ROW(col1 %s, col2 %s, col3 %s))".formatted(type, type, type))
                .binding(
                        "a", toVariantLiteral(createObjectWithSortedFields(
                                Metadata.of(List.of(utf8Slice("col1"), utf8Slice("col2"), utf8Slice("col3"))),
                                List.of(
                                        new ObjectField(0, variant),
                                        new ObjectField(1, variant),
                                        new ObjectField(2, variant))))))
                .asInstanceOf(list(Object.class))
                .containsExactly(value, value, value);
    }

    private TrinoExceptionAssert assertCastFromVariantThrows(Variant variant, String type)
    {
        return assertTrinoExceptionThrownBy(() -> assertions.expression("CAST(a as %s)".formatted(type))
                .binding("a", toVariantLiteral(variant))
                .evaluate());
    }

    private static String toVariantLiteral(Variant variant)
    {
        String hexMetadata = VarbinaryFunctions.toHex(variant.metadata().toSlice()).toStringUtf8();
        String hexValue = VarbinaryFunctions.toHex(variant.data()).toStringUtf8();
        return String.format("decode_variant(X'%s', X'%s')", hexMetadata, hexValue);
    }

    private static Variant createObjectWithSortedFields(Metadata metadata, List<ObjectField> fields)
    {
        return Variant.from(metadata, encodeObjectWithSortedFields(fields));
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
            assertThat(variant.metadata()).isEqualTo(Metadata.EMPTY_METADATA);
        }
    }
}
