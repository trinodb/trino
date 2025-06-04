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
package io.trino.plugin.hive.coercions;

import io.trino.plugin.hive.HiveTimestampPrecision;
import io.trino.plugin.hive.coercions.CoercionUtils.CoercionContext;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.hive.HiveStorageFormat.PARQUET;
import static io.trino.plugin.hive.HiveTimestampPrecision.MICROSECONDS;
import static io.trino.plugin.hive.HiveTimestampPrecision.NANOSECONDS;
import static io.trino.plugin.hive.coercions.CoercionUtils.createCoercer;
import static io.trino.plugin.hive.util.HiveTypeTranslator.toHiveType;
import static io.trino.spi.predicate.Utils.blockToNativeValue;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_PICOS;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTimestampCoercer
{
    @Test
    public void testTimestampToDate()
    {
        // before epoch
        assertTimestampToDateCoercion("1900-01-01T00:00:00.000", "1900-01-01");
        assertTimestampToDateCoercion("1958-01-01T13:18:03.123", "1958-01-01");
        // after epoch
        assertTimestampToDateCoercion("2019-03-18T10:01:17.987", "2019-03-18");
        assertTimestampToDateCoercion("2021-12-31T23:59:59.000", "2021-12-31");
        assertTimestampToDateCoercion("2021-12-31T23:59:59.999", "2021-12-31");
        assertTimestampToDateCoercion("2021-12-31T23:59:59.999999", "2021-12-31");
        assertTimestampToDateCoercion("2021-12-31T23:59:59.999999999", "2021-12-31");
        // time doubled in JVM zone
        assertTimestampToDateCoercion("2018-10-28T01:33:17.456", "2018-10-28");
        // epoch
        assertTimestampToDateCoercion("1970-01-01T00:00:00.000", "1970-01-01");
        // time gap in JVM zone
        assertTimestampToDateCoercion("1970-01-01T00:13:42.000", "1970-01-01");
        assertTimestampToDateCoercion("2018-04-01T02:13:55.123", "2018-04-01");
        // time gap in Vilnius
        assertTimestampToDateCoercion("2018-03-25T03:17:17.000", "2018-03-25");
        // time gap in Kathmandu
        assertTimestampToDateCoercion("1986-01-01T00:13:07.000", "1986-01-01");
        // before epoch with second fraction
        assertTimestampToDateCoercion("1969-12-31T23:59:59.123456", "1969-12-31");
    }

    @Test
    public void testHistoricalLongTimestampToDate()
    {
        assertThatThrownBy(() -> assertTimestampToDateCoercion("1899-12-31T23:59:59.999999999", "1899-12-31"))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Coercion on historical dates is not supported");
    }

    @Test
    public void testTimestampToVarchar()
    {
        testTimestampToVarchar("1900-01-01T00:00:00.000", "1900-01-01 00:00:00");
        testTimestampToVarchar("1958-01-01T13:18:03.123", "1958-01-01 13:18:03.123");
        // after epoch
        testTimestampToVarchar("2019-03-18T10:01:17.987", "2019-03-18 10:01:17.987");
        // time doubled in JVM zone
        testTimestampToVarchar("2018-10-28T01:33:17.456", "2018-10-28 01:33:17.456");
        // time doubled in JVM zone
        testTimestampToVarchar("2018-10-28T03:33:33.333", "2018-10-28 03:33:33.333");
        // epoch
        testTimestampToVarchar("1970-01-01T00:00:00.000", "1970-01-01 00:00:00");
        // time gap in JVM zone
        testTimestampToVarchar("1970-01-01T00:13:42.000", "1970-01-01 00:13:42");
        testTimestampToVarchar("2018-04-01T02:13:55.123", "2018-04-01 02:13:55.123");
        // time gap in Vilnius
        testTimestampToVarchar("2018-03-25T03:17:17.000", "2018-03-25 03:17:17");
        // time gap in Kathmandu
        testTimestampToVarchar("1986-01-01T00:13:07.000", "1986-01-01 00:13:07");
        // before epoch with second fraction
        testTimestampToVarchar("1969-12-31T23:59:59.123456", "1969-12-31 23:59:59.123456");
    }

    private static void testTimestampToVarchar(String timestampValue, String hiveTimestampValue)
    {
        LocalDateTime localDateTime = LocalDateTime.parse(timestampValue);
        SqlTimestamp timestamp = SqlTimestamp.fromSeconds(TIMESTAMP_PICOS.getPrecision(), localDateTime.toEpochSecond(UTC), localDateTime.get(NANO_OF_SECOND));
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, new LongTimestamp(timestamp.getEpochMicros(), timestamp.getPicosOfMicros()), createUnboundedVarcharType(), hiveTimestampValue);
    }

    @Test
    public void testVarcharToShortTimestamp()
    {
        testVarcharToShortTimestamp("1900-01-01T00:00:00.000", "1900-01-01 00:00:00");
        testVarcharToShortTimestamp("1958-01-01T13:18:03.123", "1958-01-01 13:18:03.123");
        // after epoch
        testVarcharToShortTimestamp("2019-03-18T10:01:17.987", "2019-03-18 10:01:17.987");
        // time doubled in JVM zone
        testVarcharToShortTimestamp("2018-10-28T01:33:17.456", "2018-10-28 01:33:17.456");
        // time doubled in JVM zone
        testVarcharToShortTimestamp("2018-10-28T03:33:33.333", "2018-10-28 03:33:33.333");
        // epoch
        testVarcharToShortTimestamp("1970-01-01T00:00:00.000", "1970-01-01 00:00:00");
        // time gap in JVM zone
        testVarcharToShortTimestamp("1970-01-01T00:13:42.000", "1970-01-01 00:13:42");
        testVarcharToShortTimestamp("2018-04-01T02:13:55.123", "2018-04-01 02:13:55.123");
        // time gap in Vilnius
        testVarcharToShortTimestamp("2018-03-25T03:17:17.000", "2018-03-25 03:17:17");
        // time gap in Kathmandu
        testVarcharToShortTimestamp("1986-01-01T00:13:07.000", "1986-01-01 00:13:07");
        // before epoch with second fraction
        testVarcharToShortTimestamp("1969-12-31T23:59:59.123456", "1969-12-31 23:59:59.123456");
    }

    private static void testVarcharToShortTimestamp(String timestampValue, String hiveTimestampValue)
    {
        LocalDateTime localDateTime = LocalDateTime.parse(timestampValue);
        SqlTimestamp timestamp = SqlTimestamp.fromSeconds(TIMESTAMP_MICROS.getPrecision(), localDateTime.toEpochSecond(UTC), localDateTime.get(NANO_OF_SECOND));
        assertVarcharToShortTimestampCoercions(createUnboundedVarcharType(), utf8Slice(hiveTimestampValue), TIMESTAMP_MICROS, timestamp.getEpochMicros());
    }

    @Test
    public void testVarcharToLongTimestamp()
    {
        testVarcharToLongTimestamp("1900-01-01T00:00:00.000", "1900-01-01 00:00:00");
        testVarcharToLongTimestamp("1958-01-01T13:18:03.123", "1958-01-01 13:18:03.123");
        // after epoch
        testVarcharToLongTimestamp("2019-03-18T10:01:17.987", "2019-03-18 10:01:17.987");
        // time doubled in JVM zone
        testVarcharToLongTimestamp("2018-10-28T01:33:17.456", "2018-10-28 01:33:17.456");
        // time doubled in JVM zone
        testVarcharToLongTimestamp("2018-10-28T03:33:33.333", "2018-10-28 03:33:33.333");
        // epoch
        testVarcharToLongTimestamp("1970-01-01T00:00:00.000", "1970-01-01 00:00:00");
        // time gap in JVM zone
        testVarcharToLongTimestamp("1970-01-01T00:13:42.000", "1970-01-01 00:13:42");
        testVarcharToLongTimestamp("2018-04-01T02:13:55.123", "2018-04-01 02:13:55.123");
        // time gap in Vilnius
        testVarcharToLongTimestamp("2018-03-25T03:17:17.000", "2018-03-25 03:17:17");
        // time gap in Kathmandu
        testVarcharToLongTimestamp("1986-01-01T00:13:07.000", "1986-01-01 00:13:07");
        // before epoch with second fraction
        testVarcharToLongTimestamp("1969-12-31T23:59:59.123456", "1969-12-31 23:59:59.123456");
    }

    private static void testVarcharToLongTimestamp(String timestampValue, String hiveTimestampValue)
    {
        LocalDateTime localDateTime = LocalDateTime.parse(timestampValue);
        SqlTimestamp timestamp = SqlTimestamp.fromSeconds(TIMESTAMP_PICOS.getPrecision(), localDateTime.toEpochSecond(UTC), localDateTime.get(NANO_OF_SECOND));
        assertVarcharToLongTimestampCoercions(createUnboundedVarcharType(), utf8Slice(hiveTimestampValue), TIMESTAMP_PICOS, new LongTimestamp(timestamp.getEpochMicros(), timestamp.getPicosOfMicros()));
    }

    @Test
    public void testTimestampToSmallerVarchar()
    {
        LocalDateTime localDateTime = LocalDateTime.parse("2023-04-11T05:16:12.345678876");
        SqlTimestamp timestamp = SqlTimestamp.fromSeconds(TIMESTAMP_PICOS.getPrecision(), localDateTime.toEpochSecond(UTC), localDateTime.get(NANO_OF_SECOND));
        LongTimestamp longTimestamp = new LongTimestamp(timestamp.getEpochMicros(), timestamp.getPicosOfMicros());
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(1), "2");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(2), "20");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(3), "202");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(4), "2023");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(5), "2023-");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(6), "2023-0");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(7), "2023-04");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(8), "2023-04-");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(9), "2023-04-1");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(10), "2023-04-11");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(11), "2023-04-11 ");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(12), "2023-04-11 0");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(13), "2023-04-11 05");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(14), "2023-04-11 05:");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(15), "2023-04-11 05:1");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(16), "2023-04-11 05:16");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(17), "2023-04-11 05:16:");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(18), "2023-04-11 05:16:1");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(19), "2023-04-11 05:16:12");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(20), "2023-04-11 05:16:12.");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(21), "2023-04-11 05:16:12.3");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(22), "2023-04-11 05:16:12.34");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(23), "2023-04-11 05:16:12.345");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(24), "2023-04-11 05:16:12.3456");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(25), "2023-04-11 05:16:12.34567");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(26), "2023-04-11 05:16:12.345678");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(27), "2023-04-11 05:16:12.3456788");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(28), "2023-04-11 05:16:12.34567887");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, longTimestamp, createVarcharType(29), "2023-04-11 05:16:12.345678876");
    }

    @Test
    public void testHistoricalLongTimestampToVarchar()
    {
        LocalDateTime localDateTime = LocalDateTime.parse("1899-12-31T23:59:59.999999999");
        SqlTimestamp timestamp = SqlTimestamp.fromSeconds(TIMESTAMP_PICOS.getPrecision(), localDateTime.toEpochSecond(UTC), localDateTime.get(NANO_OF_SECOND));
        assertThatThrownBy(() ->
                assertLongTimestampToVarcharCoercions(
                        TIMESTAMP_PICOS,
                        new LongTimestamp(timestamp.getEpochMicros(), timestamp.getPicosOfMicros()),
                        createUnboundedVarcharType(),
                        "1899-12-31 23:59:59.999999999"))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Coercion on historical dates is not supported");
    }

    @Test
    public void testInvalidVarcharToShortTimestamp()
    {
        testInvalidVarcharToShortTimestamp("Invalid timestamp"); // Invalid string
        testInvalidVarcharToShortTimestamp("2022"); // Partial timestamp value
        testInvalidVarcharToShortTimestamp("2001-04-01T00:13:42.000"); // ISOFormat date
        testInvalidVarcharToShortTimestamp("2001-14-01 00:13:42.000"); // Invalid month
        testInvalidVarcharToShortTimestamp("2001-01-32 00:13:42.000"); // Invalid day
        testInvalidVarcharToShortTimestamp("2001-04-01 23:59:60.000"); // Invalid second
        testInvalidVarcharToShortTimestamp("2001-04-01 23:60:01.000"); // Invalid minute
        testInvalidVarcharToShortTimestamp("2001-04-01 27:01:01.000"); // Invalid hour
    }

    private static void testInvalidVarcharToShortTimestamp(String invalidValue)
    {
        assertVarcharToShortTimestampCoercions(createUnboundedVarcharType(), utf8Slice(invalidValue), TIMESTAMP_MICROS, null);
    }

    @Test
    public void testInvalidVarcharLongTimestamp()
    {
        testInvalidVarcharLongTimestamp("Invalid timestamp"); // Invalid string
        testInvalidVarcharLongTimestamp("2022"); // Partial timestamp value
        testInvalidVarcharLongTimestamp("2001-04-01T00:13:42.000"); // ISOFormat date
        testInvalidVarcharLongTimestamp("2001-14-01 00:13:42.000"); // Invalid month
        testInvalidVarcharLongTimestamp("2001-01-32 00:13:42.000"); // Invalid day
        testInvalidVarcharLongTimestamp("2001-04-01 23:59:60.000"); // Invalid second
        testInvalidVarcharLongTimestamp("2001-04-01 23:60:01.000"); // Invalid minute
        testInvalidVarcharLongTimestamp("2001-04-01 27:01:01.000"); // Invalid hour
    }

    private static void testInvalidVarcharLongTimestamp(String invalidValue)
    {
        assertVarcharToLongTimestampCoercions(createUnboundedVarcharType(), utf8Slice(invalidValue), TIMESTAMP_MICROS, null);
    }

    @Test
    public void testHistoricalVarcharToShortTimestamp()
    {
        LocalDateTime localDateTime = LocalDateTime.parse("1899-12-31T23:59:59.999999");
        SqlTimestamp timestamp = SqlTimestamp.fromSeconds(TIMESTAMP_MICROS.getPrecision(), localDateTime.toEpochSecond(UTC), localDateTime.get(NANO_OF_SECOND));
        assertThatThrownBy(() ->
                assertVarcharToShortTimestampCoercions(
                        createUnboundedVarcharType(),
                        utf8Slice("1899-12-31 23:59:59.999999"),
                        TIMESTAMP_MICROS,
                        timestamp.getEpochMicros()))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Coercion on historical dates is not supported");
    }

    @Test
    public void testHistoricalVarcharToLongTimestamp()
    {
        LocalDateTime localDateTime = LocalDateTime.parse("1899-12-31T23:59:59.999999");
        SqlTimestamp timestamp = SqlTimestamp.fromSeconds(TIMESTAMP_PICOS.getPrecision(), localDateTime.toEpochSecond(UTC), localDateTime.get(NANO_OF_SECOND));
        assertThatThrownBy(() -> assertVarcharToShortTimestampCoercions(
                createUnboundedVarcharType(),
                utf8Slice("1899-12-31 23:59:59.999999"),
                TIMESTAMP_PICOS,
                timestamp.getEpochMicros()))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Coercion on historical dates is not supported");
    }

    public static void assertLongTimestampToVarcharCoercions(TimestampType fromType, LongTimestamp valueToBeCoerced, VarcharType toType, String expectedValue)
    {
        assertCoercions(fromType, valueToBeCoerced, toType, utf8Slice(expectedValue), NANOSECONDS);
    }

    public static void assertVarcharToShortTimestampCoercions(Type fromType, Object valueToBeCoerced, Type toType, Object expectedValue)
    {
        assertCoercions(fromType, valueToBeCoerced, toType, expectedValue, MICROSECONDS);
    }

    public static void assertVarcharToLongTimestampCoercions(Type fromType, Object valueToBeCoerced, Type toType, Object expectedValue)
    {
        assertCoercions(fromType, valueToBeCoerced, toType, expectedValue, NANOSECONDS);
    }

    public static void assertCoercions(Type fromType, Object valueToBeCoerced, Type toType, Object expectedValue, HiveTimestampPrecision timestampPrecision)
    {
        Block coercedValue = createCoercer(TESTING_TYPE_MANAGER, toHiveType(fromType), toHiveType(toType), new CoercionContext(timestampPrecision, PARQUET)).orElseThrow()
                .apply(nativeValueToBlock(fromType, valueToBeCoerced));
        assertThat(blockToNativeValue(toType, coercedValue))
                .isEqualTo(expectedValue);
    }

    private static void assertTimestampToDateCoercion(String timestampAsString, String expectedDate)
    {
        LocalDateTime localDateTime = LocalDateTime.parse(timestampAsString);
        SqlTimestamp timestamp = SqlTimestamp.fromSeconds(TIMESTAMP_PICOS.getPrecision(), localDateTime.toEpochSecond(UTC), localDateTime.get(NANO_OF_SECOND));
        assertCoercions(TIMESTAMP_PICOS, new LongTimestamp(timestamp.getEpochMicros(), timestamp.getPicosOfMicros()), DATE, LocalDate.parse(expectedDate).toEpochDay(), NANOSECONDS);
    }
}
