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
package io.trino.plugin.iceberg.util;

import io.trino.spi.TrinoException;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTimestamps
{
    private static final long MIN_NANO_EPOCH_MICROS = floorDiv(Long.MIN_VALUE, NANOSECONDS_PER_MICROSECOND);
    private static final int MIN_NANO_OF_MICRO = (int) floorMod(Long.MIN_VALUE, NANOSECONDS_PER_MICROSECOND);
    private static final long MAX_NANO_EPOCH_MICROS = floorDiv(Long.MAX_VALUE, NANOSECONDS_PER_MICROSECOND);
    private static final int MAX_NANO_OF_MICRO = (int) floorMod(Long.MAX_VALUE, NANOSECONDS_PER_MICROSECOND);

    private static final long MIN_NANO_EPOCH_MILLIS = floorDiv(Long.MIN_VALUE, NANOSECONDS_PER_MILLISECOND);
    private static final int MIN_NANO_OF_MILLI = (int) floorMod(Long.MIN_VALUE, NANOSECONDS_PER_MILLISECOND);
    private static final long MAX_NANO_EPOCH_MILLIS = floorDiv(Long.MAX_VALUE, NANOSECONDS_PER_MILLISECOND);
    private static final int MAX_NANO_OF_MILLI = (int) floorMod(Long.MAX_VALUE, NANOSECONDS_PER_MILLISECOND);

    @Test
    public void testTimestampNanosRoundTripAtLongBounds()
    {
        assertThat(Timestamps.timestampToNanos(Timestamps.timestampFromNanos(Long.MIN_VALUE))).isEqualTo(Long.MIN_VALUE);
        assertThat(Timestamps.timestampToNanos(Timestamps.timestampFromNanos(Long.MAX_VALUE))).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    public void testTimestampTzNanosRoundTripAtLongBounds()
    {
        assertThat(Timestamps.timestampTzToNanos(Timestamps.timestampTzFromNanos(Long.MIN_VALUE))).isEqualTo(Long.MIN_VALUE);
        assertThat(Timestamps.timestampTzToNanos(Timestamps.timestampTzFromNanos(Long.MAX_VALUE))).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    public void testTimestampNanosRejectsOutOfRangeBoundaryTuples()
    {
        assertThatThrownBy(() -> Timestamps.timestampToNanos(new LongTimestamp(MIN_NANO_EPOCH_MICROS, (MIN_NANO_OF_MICRO - 1) * PICOSECONDS_PER_NANOSECOND)))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Timestamp value is outside the range supported by Iceberg nano timestamps");

        assertThatThrownBy(() -> Timestamps.timestampToNanos(new LongTimestamp(MAX_NANO_EPOCH_MICROS, (MAX_NANO_OF_MICRO + 1) * PICOSECONDS_PER_NANOSECOND)))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Timestamp value is outside the range supported by Iceberg nano timestamps");
    }

    @Test
    public void testTimestampTzNanosRejectsOutOfRangeBoundaryTuples()
    {
        assertThatThrownBy(() -> Timestamps.timestampTzToNanos(LongTimestampWithTimeZone.fromEpochMillisAndFraction(MIN_NANO_EPOCH_MILLIS, (MIN_NANO_OF_MILLI - 1) * PICOSECONDS_PER_NANOSECOND, UTC_KEY)))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Timestamp value is outside the range supported by Iceberg nano timestamps");

        assertThatThrownBy(() -> Timestamps.timestampTzToNanos(LongTimestampWithTimeZone.fromEpochMillisAndFraction(MAX_NANO_EPOCH_MILLIS, (MAX_NANO_OF_MILLI + 1) * PICOSECONDS_PER_NANOSECOND, UTC_KEY)))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Timestamp value is outside the range supported by Iceberg nano timestamps");
    }
}
