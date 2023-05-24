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
package io.trino.parquet;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.trino.plugin.base.type.DecodedTimestamp;
import io.trino.spi.TrinoException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;

import static com.google.common.base.Verify.verify;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.SECONDS_PER_DAY;
import static java.lang.StrictMath.floorDiv;
import static java.lang.StrictMath.floorMod;
import static java.lang.StrictMath.toIntExact;

/**
 * Utility class for decoding INT96 encoded parquet timestamp to timestamp millis in GMT.
 * <p>
 */
public final class ParquetTimestampUtils
{
    public static final int JULIAN_EPOCH_OFFSET_DAYS = 2_440_588;

    private ParquetTimestampUtils() {}

    /**
     * Returns GMT timestamp from binary encoded parquet timestamp (12 bytes - julian date + time of day nanos).
     *
     * @param timestampBinary INT96 parquet timestamp
     */
    public static DecodedTimestamp decodeInt96Timestamp(Binary timestampBinary)
    {
        if (timestampBinary.length() != 12) {
            throw new TrinoException(NOT_SUPPORTED, "Parquet timestamp must be 12 bytes, actual " + timestampBinary.length());
        }
        byte[] bytes = timestampBinary.getBytes();

        // little endian encoding - need to invert byte order
        long timeOfDayNanos = Longs.fromBytes(bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]);
        int julianDay = Ints.fromBytes(bytes[11], bytes[10], bytes[9], bytes[8]);

        return decodeInt96Timestamp(timeOfDayNanos, julianDay);
    }

    public static DecodedTimestamp decodeInt96Timestamp(long timeOfDayNanos, int julianDay)
    {
        verify(timeOfDayNanos >= 0 && timeOfDayNanos < NANOSECONDS_PER_DAY, "Invalid timeOfDayNanos: %s", timeOfDayNanos);
        long epochSeconds = (julianDay - JULIAN_EPOCH_OFFSET_DAYS) * SECONDS_PER_DAY + timeOfDayNanos / NANOSECONDS_PER_SECOND;
        return new DecodedTimestamp(epochSeconds, (int) (timeOfDayNanos % NANOSECONDS_PER_SECOND));
    }

    public static DecodedTimestamp decodeInt64Timestamp(long timestamp, LogicalTypeAnnotation.TimeUnit precision)
    {
        long toSecondsConversion;
        long toNanosConversion;
        switch (precision) {
            case MILLIS:
                toSecondsConversion = MILLISECONDS_PER_SECOND;
                toNanosConversion = NANOSECONDS_PER_MILLISECOND;
                break;
            case MICROS:
                toSecondsConversion = MICROSECONDS_PER_SECOND;
                toNanosConversion = NANOSECONDS_PER_MICROSECOND;
                break;
            case NANOS:
                toSecondsConversion = NANOSECONDS_PER_SECOND;
                toNanosConversion = 1;
                break;
            default:
                throw new TrinoException(NOT_SUPPORTED, "Unsupported Parquet timestamp time unit " + precision);
        }
        long epochSeconds = floorDiv(timestamp, toSecondsConversion);
        long fractionalSecond = floorMod(timestamp, toSecondsConversion);
        int nanosOfSecond = toIntExact(fractionalSecond * toNanosConversion);

        return new DecodedTimestamp(epochSeconds, nanosOfSecond);
    }
}
