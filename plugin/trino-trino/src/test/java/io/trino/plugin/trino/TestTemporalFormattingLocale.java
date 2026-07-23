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
import io.trino.spi.type.LongTimeWithTimeZone;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeZoneKey;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.lang.reflect.Proxy;
import java.sql.PreparedStatement;
import java.time.Instant;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static org.assertj.core.api.Assertions.assertThat;

@Isolated("Changes the JVM default FORMAT locale")
final class TestTemporalFormattingLocale
{
    private static final long TIME_PICOS = 54_180_123_456_789_012L;

    @Test
    void testTemporalParametersUseAsciiDigitsWithNonLatinDefaultLocale()
            throws Exception
    {
        Locale originalLocale = Locale.getDefault(Locale.Category.FORMAT);
        try {
            Locale.setDefault(Locale.Category.FORMAT, Locale.forLanguageTag("ar-EG"));

            LongTimestampWithTimeZone timestampWithTimeZone = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                    Instant.parse("2020-02-12T15:03:00.123Z").toEpochMilli(),
                    456_789_012,
                    TimeZoneKey.UTC_KEY);
            assertThat(TimestampWithTimeZoneTransport.formatLongParameterValue(timestampWithTimeZone, createTimestampWithTimeZoneType(12)))
                    .isEqualTo("2020-02-12 15:03:00.123456789012 +00:00");

            AtomicReference<String> boundValue = new AtomicReference<>();
            PreparedStatement statement = recordingPreparedStatement(boundValue);

            LongWriteFunction timeFunction = TemporalTransportCodec.timeTransportWriteFunction(createTimeType(12));
            timeFunction.set(statement, 1, TIME_PICOS);
            assertThat(boundValue).hasValue("15:03:00.123456789012");

            ObjectWriteFunction timestampFunction = TemporalTransportCodec.longTimestampTransportWriteFunction(createTimestampType(12));
            timestampFunction.set(statement, 1, new LongTimestamp(
                    Instant.parse("2020-02-12T15:03:00Z").getEpochSecond() * 1_000_000 + 123_456,
                    789_012));
            assertThat(boundValue).hasValue("2020-02-12 15:03:00.123456789012");

            ObjectWriteFunction timeWithTimeZoneFunction = TemporalTransportCodec.longTimeWithTimeZoneTransportWriteFunction(createTimeWithTimeZoneType(12));
            timeWithTimeZoneFunction.set(statement, 1, new LongTimeWithTimeZone(TIME_PICOS, 330));
            assertThat(boundValue).hasValue("15:03:00.123456789012+05:30");
        }
        finally {
            Locale.setDefault(Locale.Category.FORMAT, originalLocale);
        }
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
