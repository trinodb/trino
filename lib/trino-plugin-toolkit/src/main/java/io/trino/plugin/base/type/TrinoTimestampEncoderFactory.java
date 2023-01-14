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
package io.trino.plugin.base.type;

import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;
import org.joda.time.DateTimeZone;

import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static java.lang.Math.addExact;
import static java.lang.Math.multiplyExact;
import static java.util.Objects.requireNonNull;

public final class TrinoTimestampEncoderFactory
{
    private TrinoTimestampEncoderFactory() {}

    public static TrinoTimestampEncoder<? extends Comparable<?>> createTimestampEncoder(TimestampType type, DateTimeZone timeZone)
    {
        requireNonNull(type, "type is null");
        requireNonNull(timeZone, "timeZoneKey is null");

        if (type.isShort()) {
            return new ShortTimestampEncoder(type, timeZone);
        }
        return new LongTimestampEncoder(type, timeZone);
    }

    // copied from io.trino.type.DateTimes
    static LongTimestamp longTimestamp(long epochSecond, long fractionInPicos)
    {
        return new LongTimestamp(
                addExact(multiplyExact(epochSecond, MICROSECONDS_PER_SECOND), fractionInPicos / PICOSECONDS_PER_MICROSECOND),
                (int) (fractionInPicos % PICOSECONDS_PER_MICROSECOND));
    }
}
