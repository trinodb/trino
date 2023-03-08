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
package io.trino.plugin.influxdb;

import io.trino.spi.type.LongTimestamp;

import java.time.Instant;

import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static java.time.temporal.ChronoField.MICRO_OF_SECOND;

public final class TimestampUtils
{
    private TimestampUtils() {}

    public static LongTimestamp longTimestamp(Instant start)
    {
        return new LongTimestamp(
                start.getEpochSecond() * MICROSECONDS_PER_SECOND + start.getLong(MICRO_OF_SECOND),
                (start.getNano() % NANOSECONDS_PER_MICROSECOND) * PICOSECONDS_PER_NANOSECOND);
    }

    public static long unixTimestamp(LongTimestamp timestamp)
    {
        return timestamp.getEpochMicros() * NANOSECONDS_PER_MICROSECOND +
                timestamp.getPicosOfMicro() / PICOSECONDS_PER_NANOSECOND;
    }
}
