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
package io.trino.plugin.trino;

import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.type.DateTimeEncoding;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampWithTimeZoneType;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.PreparedStatement;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestTimestampWithTimeZoneTransport
{
    private static final TimeZoneKey WARSAW = TimeZoneKey.getTimeZoneKey("Europe/Warsaw");
    private static final long EARLIER_FOLD_MILLIS = Instant.parse("2022-10-30T00:30:00Z").toEpochMilli();
    private static final long LATER_FOLD_MILLIS = Instant.parse("2022-10-30T01:30:00Z").toEpochMilli();

    @Test
    void testReadExpression()
    {
        assertThat(TimestampWithTimeZoneTransport.readExpression("\"created_at\""))
                .isEqualTo("to_iso8601(at_timezone(\"created_at\", 'UTC')) || '|' || format_datetime(\"created_at\", 'ZZZ')");
    }

    @Test
    void testParsesEveryPrecision()
    {
        String digits = "123456789012";
        long epochSecond = Instant.parse("2020-02-12T15:03:00Z").getEpochSecond();
        for (int precision = 0; precision <= 12; precision++) {
            String fraction = precision == 0 ? "" : "." + digits.substring(0, precision);
            long fractionPicos = precision == 0
                    ? 0
                    : Long.parseLong((digits.substring(0, precision) + "000000000000").substring(0, 12));
            String value = "2020-02-12T15:03:00" + fraction + "Z|UTC";

            if (precision <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
                long parsed = TimestampWithTimeZoneTransport.parseShortTimestampWithTimeZone(value);
                assertThat(DateTimeEncoding.unpackMillisUtc(parsed))
                        .as("precision %s", precision)
                        .isEqualTo(epochSecond * 1_000 + fractionPicos / 1_000_000_000L);
                assertThat(DateTimeEncoding.unpackZoneKey(parsed)).isEqualTo(TimeZoneKey.UTC_KEY);
            }
            else {
                LongTimestampWithTimeZone parsed = TimestampWithTimeZoneTransport.parseLongTimestampWithTimeZone(value);
                assertThat(parsed.getEpochMillis())
                        .as("precision %s", precision)
                        .isEqualTo(epochSecond * 1_000 + fractionPicos / 1_000_000_000L);
                assertThat(parsed.getPicosOfMilli()).isEqualTo((int) (fractionPicos % 1_000_000_000L));
                assertThat(TimeZoneKey.getTimeZoneKey(parsed.getTimeZoneKey())).isEqualTo(TimeZoneKey.UTC_KEY);
            }
        }
    }

    @Test
    void testDstFoldPreservesInstantAndZone()
    {
        long earlier = TimestampWithTimeZoneTransport.parseShortTimestampWithTimeZone(
                "2022-10-30T02:30:00.123+02:00|Europe/Warsaw");
        long later = TimestampWithTimeZoneTransport.parseShortTimestampWithTimeZone(
                "2022-10-30T02:30:00.123+01:00|Europe/Warsaw");

        assertThat(DateTimeEncoding.unpackMillisUtc(earlier)).isEqualTo(EARLIER_FOLD_MILLIS + 123);
        assertThat(DateTimeEncoding.unpackMillisUtc(later)).isEqualTo(LATER_FOLD_MILLIS + 123);
        assertThat(DateTimeEncoding.unpackZoneKey(earlier)).isEqualTo(WARSAW);
        assertThat(DateTimeEncoding.unpackZoneKey(later)).isEqualTo(WARSAW);
        assertThat(DateTimeEncoding.unpackMillisUtc(later) - DateTimeEncoding.unpackMillisUtc(earlier))
                .isEqualTo(3_600_000);
    }

    @Test
    void testLongTimestampPreservesPicoseconds()
    {
        LongTimestampWithTimeZone earlier = TimestampWithTimeZoneTransport.parseLongTimestampWithTimeZone(
                "2022-10-30T02:30:00.123456789012+02:00|Europe/Warsaw");
        LongTimestampWithTimeZone later = TimestampWithTimeZoneTransport.parseLongTimestampWithTimeZone(
                "2022-10-30T02:30:00.123456789012+01:00|Europe/Warsaw");

        assertThat(earlier.getEpochMillis()).isEqualTo(EARLIER_FOLD_MILLIS + 123);
        assertThat(later.getEpochMillis()).isEqualTo(LATER_FOLD_MILLIS + 123);
        assertThat(later.getEpochMillis() - earlier.getEpochMillis()).isEqualTo(3_600_000);
        assertThat(earlier.getPicosOfMilli()).isEqualTo(456_789_012);
        assertThat(later.getPicosOfMilli()).isEqualTo(456_789_012);
        assertThat(TimeZoneKey.getTimeZoneKey(earlier.getTimeZoneKey())).isEqualTo(WARSAW);
        assertThat(TimeZoneKey.getTimeZoneKey(later.getTimeZoneKey())).isEqualTo(WARSAW);
    }

    @Test
    void testFixedOffsetZone()
    {
        long parsed = TimestampWithTimeZoneTransport.parseShortTimestampWithTimeZone(
                "2020-02-12T15:03:00.1+05:30|+05:30");

        assertThat(DateTimeEncoding.unpackMillisUtc(parsed))
                .isEqualTo(Instant.parse("2020-02-12T09:33:00.100Z").toEpochMilli());
        assertThat(DateTimeEncoding.unpackZoneKey(parsed).getId()).isEqualTo("+05:30");
    }

    @Test
    void testHistoricalRegionOffsetUsesUtcParameterTransport()
    {
        TimeZoneKey paris = TimeZoneKey.getTimeZoneKey("Europe/Paris");
        long value = DateTimeEncoding.packDateTimeWithZone(Instant.parse("1890-01-01T00:00:00Z").toEpochMilli(), paris);

        assertThat(TimestampWithTimeZoneTransport.formatShortParameterValue(value, createTimestampWithTimeZoneType(3)))
                .isEqualTo("1890-01-01 00:00:00.000 +00:00");
        assertThat(TimestampWithTimeZoneTransport.shortZoneId(value)).isEqualTo("Europe/Paris");
    }

    @Test
    void testParameterValuesUseUtcInstantTransport()
    {
        TimestampWithTimeZoneType shortType = createTimestampWithTimeZoneType(3);
        long earlier = DateTimeEncoding.packDateTimeWithZone(EARLIER_FOLD_MILLIS + 123, WARSAW);
        long later = DateTimeEncoding.packDateTimeWithZone(LATER_FOLD_MILLIS + 123, WARSAW);

        assertThat(TimestampWithTimeZoneTransport.formatShortParameterValue(earlier, shortType))
                .isEqualTo("2022-10-30 00:30:00.123 +00:00");
        assertThat(TimestampWithTimeZoneTransport.formatShortParameterValue(later, shortType))
                .isEqualTo("2022-10-30 01:30:00.123 +00:00");
        assertThat(TimestampWithTimeZoneTransport.shortZoneId(later)).isEqualTo("Europe/Warsaw");

        TimestampWithTimeZoneType longType = createTimestampWithTimeZoneType(12);
        LongTimestampWithTimeZone longValue = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                LATER_FOLD_MILLIS + 123,
                456_789_012,
                WARSAW);
        assertThat(TimestampWithTimeZoneTransport.formatLongParameterValue(longValue, longType))
                .isEqualTo("2022-10-30 01:30:00.123456789012 +00:00");
        assertThat(TimestampWithTimeZoneTransport.longZoneId(longValue)).isEqualTo("Europe/Warsaw");
    }

    @Test
    void testPredicateWriteFunctions()
            throws Exception
    {
        AtomicReference<String> boundValue = new AtomicReference<>();
        PreparedStatement statement = recordingPreparedStatement(boundValue);

        TimestampWithTimeZoneType shortType = createTimestampWithTimeZoneType(3);
        LongWriteFunction shortFunction = TimestampWithTimeZoneTransport.shortPredicateWriteFunction(shortType);
        shortFunction.set(statement, 1, DateTimeEncoding.packDateTimeWithZone(LATER_FOLD_MILLIS + 123, WARSAW));
        assertThat(shortFunction.getBindExpression()).isEqualTo("CAST(? AS timestamp(3) with time zone)");
        assertThat(boundValue).hasValue("2022-10-30 01:30:00.123 +00:00");

        TimestampWithTimeZoneType longType = createTimestampWithTimeZoneType(12);
        ObjectWriteFunction longFunction = TimestampWithTimeZoneTransport.longPredicateWriteFunction(longType);
        longFunction.set(statement, 1, LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                LATER_FOLD_MILLIS + 123,
                456_789_012,
                WARSAW));
        assertThat(longFunction.getBindExpression()).isEqualTo("CAST(? AS timestamp(12) with time zone)");
        assertThat(longFunction.getJavaType()).isEqualTo(LongTimestampWithTimeZone.class);
        assertThat(boundValue).hasValue("2022-10-30 01:30:00.123456789012 +00:00");
    }

    @Test
    void testRejectsAmbiguousWallTime()
    {
        assertThatThrownBy(() -> TimestampWithTimeZoneTransport.parseShortTimestampWithTimeZone(
                "2022-10-30 02:30:00 Europe/Warsaw"))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Invalid timestamp with time zone transport value");
    }

    private static PreparedStatement recordingPreparedStatement(AtomicReference<String> boundValue)
    {
        return (PreparedStatement) Proxy.newProxyInstance(
                PreparedStatement.class.getClassLoader(),
                new Class<?>[] {PreparedStatement.class},
                (_, method, arguments) -> {
                    if (method.getName().equals("setString")) {
                        boundValue.set((String) arguments[1]);
                    }
                    return null;
                });
    }
}
