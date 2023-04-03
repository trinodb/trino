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

import io.airlift.slice.Slices;
import io.trino.plugin.hive.HiveTimestampPrecision;
import io.trino.spi.block.Block;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.LocalDateTime;

import static io.trino.plugin.hive.HivePageSource.createCoercer;
import static io.trino.plugin.hive.HiveTimestampPrecision.MICROSECONDS;
import static io.trino.plugin.hive.HiveTimestampPrecision.NANOSECONDS;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static io.trino.spi.predicate.Utils.blockToNativeValue;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_PICOS;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTimestampCoercer
{
    @Test(dataProvider = "timestampValuesProvider")
    public void testShortTimestampToVarchar(String timestampValue, String hiveTimestampValue)
    {
        LocalDateTime localDateTime = LocalDateTime.parse(timestampValue);
        SqlTimestamp timestamp = SqlTimestamp.fromSeconds(TIMESTAMP_MICROS.getPrecision(), localDateTime.toEpochSecond(UTC), localDateTime.get(NANO_OF_SECOND));
        assertShortTimestampToVarcharCoercions(TIMESTAMP_MICROS, timestamp.getEpochMicros(), createUnboundedVarcharType(), hiveTimestampValue);
    }

    @Test(dataProvider = "timestampValuesProvider")
    public void testLongTimestampToVarchar(String timestampValue, String hiveTimestampValue)
    {
        LocalDateTime localDateTime = LocalDateTime.parse(timestampValue);
        SqlTimestamp timestamp = SqlTimestamp.fromSeconds(TIMESTAMP_PICOS.getPrecision(), localDateTime.toEpochSecond(UTC), localDateTime.get(NANO_OF_SECOND));
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, new LongTimestamp(timestamp.getEpochMicros(), timestamp.getPicosOfMicros()), createUnboundedVarcharType(), hiveTimestampValue);
    }

    @Test
    public void testShortTimestampToSmallerVarchar()
    {
        LocalDateTime localDateTime = LocalDateTime.parse("2023-04-11T05:16:12.345678");
        SqlTimestamp timestamp = SqlTimestamp.fromSeconds(TIMESTAMP_MICROS.getPrecision(), localDateTime.toEpochSecond(UTC), localDateTime.get(NANO_OF_SECOND));
        assertShortTimestampToVarcharCoercions(TIMESTAMP_MICROS, timestamp.getEpochMicros(), createVarcharType(1), "2");
        assertShortTimestampToVarcharCoercions(TIMESTAMP_MICROS, timestamp.getEpochMicros(), createVarcharType(2), "20");
        assertShortTimestampToVarcharCoercions(TIMESTAMP_MICROS, timestamp.getEpochMicros(), createVarcharType(3), "202");
        assertShortTimestampToVarcharCoercions(TIMESTAMP_MICROS, timestamp.getEpochMicros(), createVarcharType(4), "2023");
        assertShortTimestampToVarcharCoercions(TIMESTAMP_MICROS, timestamp.getEpochMicros(), createVarcharType(5), "2023-");
        assertShortTimestampToVarcharCoercions(TIMESTAMP_MICROS, timestamp.getEpochMicros(), createVarcharType(6), "2023-0");
        assertShortTimestampToVarcharCoercions(TIMESTAMP_MICROS, timestamp.getEpochMicros(), createVarcharType(7), "2023-04");
        assertShortTimestampToVarcharCoercions(TIMESTAMP_MICROS, timestamp.getEpochMicros(), createVarcharType(8), "2023-04-");
        assertShortTimestampToVarcharCoercions(TIMESTAMP_MICROS, timestamp.getEpochMicros(), createVarcharType(9), "2023-04-1");
        assertShortTimestampToVarcharCoercions(TIMESTAMP_MICROS, timestamp.getEpochMicros(), createVarcharType(10), "2023-04-11");
        assertShortTimestampToVarcharCoercions(TIMESTAMP_MICROS, timestamp.getEpochMicros(), createVarcharType(11), "2023-04-11 ");
        assertShortTimestampToVarcharCoercions(TIMESTAMP_MICROS, timestamp.getEpochMicros(), createVarcharType(12), "2023-04-11 0");
        assertShortTimestampToVarcharCoercions(TIMESTAMP_MICROS, timestamp.getEpochMicros(), createVarcharType(13), "2023-04-11 05");
        assertShortTimestampToVarcharCoercions(TIMESTAMP_MICROS, timestamp.getEpochMicros(), createVarcharType(14), "2023-04-11 05:");
        assertShortTimestampToVarcharCoercions(TIMESTAMP_MICROS, timestamp.getEpochMicros(), createVarcharType(15), "2023-04-11 05:1");
        assertShortTimestampToVarcharCoercions(TIMESTAMP_MICROS, timestamp.getEpochMicros(), createVarcharType(16), "2023-04-11 05:16");
        assertShortTimestampToVarcharCoercions(TIMESTAMP_MICROS, timestamp.getEpochMicros(), createVarcharType(17), "2023-04-11 05:16:");
        assertShortTimestampToVarcharCoercions(TIMESTAMP_MICROS, timestamp.getEpochMicros(), createVarcharType(18), "2023-04-11 05:16:1");
        assertShortTimestampToVarcharCoercions(TIMESTAMP_MICROS, timestamp.getEpochMicros(), createVarcharType(19), "2023-04-11 05:16:12");
        assertShortTimestampToVarcharCoercions(TIMESTAMP_MICROS, timestamp.getEpochMicros(), createVarcharType(20), "2023-04-11 05:16:12.");
        assertShortTimestampToVarcharCoercions(TIMESTAMP_MICROS, timestamp.getEpochMicros(), createVarcharType(21), "2023-04-11 05:16:12.3");
        assertShortTimestampToVarcharCoercions(TIMESTAMP_MICROS, timestamp.getEpochMicros(), createVarcharType(22), "2023-04-11 05:16:12.34");
        assertShortTimestampToVarcharCoercions(TIMESTAMP_MICROS, timestamp.getEpochMicros(), createVarcharType(23), "2023-04-11 05:16:12.345");
        assertShortTimestampToVarcharCoercions(TIMESTAMP_MICROS, timestamp.getEpochMicros(), createVarcharType(24), "2023-04-11 05:16:12.3456");
        assertShortTimestampToVarcharCoercions(TIMESTAMP_MICROS, timestamp.getEpochMicros(), createVarcharType(25), "2023-04-11 05:16:12.34567");
        assertShortTimestampToVarcharCoercions(TIMESTAMP_MICROS, timestamp.getEpochMicros(), createVarcharType(26), "2023-04-11 05:16:12.345678");
        assertShortTimestampToVarcharCoercions(TIMESTAMP_MICROS, timestamp.getEpochMicros(), createVarcharType(27), "2023-04-11 05:16:12.345678");
    }

    @Test
    public void testLongTimestampToSmallerVarchar()
    {
        LocalDateTime localDateTime = LocalDateTime.parse("2023-04-11T05:16:12.345678876");
        SqlTimestamp timestamp = SqlTimestamp.fromSeconds(TIMESTAMP_PICOS.getPrecision(), localDateTime.toEpochSecond(UTC), localDateTime.get(NANO_OF_SECOND));
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, new LongTimestamp(timestamp.getEpochMicros(), timestamp.getPicosOfMicros()), createVarcharType(27), "2023-04-11 05:16:12.3456788");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, new LongTimestamp(timestamp.getEpochMicros(), timestamp.getPicosOfMicros()), createVarcharType(28), "2023-04-11 05:16:12.34567887");
        assertLongTimestampToVarcharCoercions(TIMESTAMP_PICOS, new LongTimestamp(timestamp.getEpochMicros(), timestamp.getPicosOfMicros()), createVarcharType(29), "2023-04-11 05:16:12.345678876");
    }

    @DataProvider
    public Object[][] timestampValuesProvider()
    {
        return new Object[][] {
                // before epoch
                {"1958-01-01T13:18:03.123", "1958-01-01 13:18:03.123"},
                // after epoch
                {"2019-03-18T10:01:17.987", "2019-03-18 10:01:17.987"},
                // time doubled in JVM zone
                {"2018-10-28T01:33:17.456", "2018-10-28 01:33:17.456"},
                // time doubled in JVM zone
                {"2018-10-28T03:33:33.333", "2018-10-28 03:33:33.333"},
                // epoch
                {"1970-01-01T00:00:00.000", "1970-01-01 00:00:00"},
                // time gap in JVM zone
                {"1970-01-01T00:13:42.000", "1970-01-01 00:13:42"},
                {"2018-04-01T02:13:55.123", "2018-04-01 02:13:55.123"},
                // time gap in Vilnius
                {"2018-03-25T03:17:17.000", "2018-03-25 03:17:17"},
                // time gap in Kathmandu
                {"1986-01-01T00:13:07.000", "1986-01-01 00:13:07"},
                // before epoch with second fraction
                {"1969-12-31T23:59:59.123456", "1969-12-31 23:59:59.123456"}
        };
    }

    public static void assertShortTimestampToVarcharCoercions(TimestampType fromType, Long valueToBeCoerced, VarcharType toType, String expectedValue)
    {
        assertCoercions(fromType, valueToBeCoerced, toType, Slices.utf8Slice(expectedValue), MICROSECONDS);
    }

    public static void assertLongTimestampToVarcharCoercions(TimestampType fromType, LongTimestamp valueToBeCoerced, VarcharType toType, String expectedValue)
    {
        assertCoercions(fromType, valueToBeCoerced, toType, Slices.utf8Slice(expectedValue), NANOSECONDS);
    }

    public static void assertCoercions(Type fromType, Object valueToBeCoerced, Type toType, Object expectedValue, HiveTimestampPrecision timestampPrecision)
    {
        Block coercedValue = createCoercer(TESTING_TYPE_MANAGER, toHiveType(fromType), toHiveType(toType), timestampPrecision).orElseThrow()
                .apply(nativeValueToBlock(fromType, valueToBeCoerced));
        assertThat(blockToNativeValue(toType, coercedValue))
                .isEqualTo(expectedValue);
    }
}
