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
package io.prestosql.parquet;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.prestosql.plugin.base.type.DecodedTimestamp;
import io.prestosql.spi.PrestoException;
import org.apache.parquet.io.api.Binary;

import static com.google.common.base.Verify.verify;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.Timestamps.NANOSECONDS_PER_DAY;
import static io.prestosql.spi.type.Timestamps.NANOSECONDS_PER_SECOND;
import static io.prestosql.spi.type.Timestamps.SECONDS_PER_DAY;

/**
 * Utility class for decoding INT96 encoded parquet timestamp to timestamp millis in GMT.
 * <p>
 */
public final class ParquetTimestampUtils
{
    @VisibleForTesting
    static final int JULIAN_EPOCH_OFFSET_DAYS = 2_440_588;

    private ParquetTimestampUtils() {}

    /**
     * Returns GMT timestamp from binary encoded parquet timestamp (12 bytes - julian date + time of day nanos).
     *
     * @param timestampBinary INT96 parquet timestamp
     */
    public static DecodedTimestamp decode(Binary timestampBinary)
    {
        if (timestampBinary.length() != 12) {
            throw new PrestoException(NOT_SUPPORTED, "Parquet timestamp must be 12 bytes, actual " + timestampBinary.length());
        }
        byte[] bytes = timestampBinary.getBytes();

        // little endian encoding - need to invert byte order
        long timeOfDayNanos = Longs.fromBytes(bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]);
        verify(timeOfDayNanos >= 0 && timeOfDayNanos < NANOSECONDS_PER_DAY, "Invalid timeOfDayNanos: %s", timeOfDayNanos);
        int julianDay = Ints.fromBytes(bytes[11], bytes[10], bytes[9], bytes[8]);

        long epochSeconds = (julianDay - JULIAN_EPOCH_OFFSET_DAYS) * SECONDS_PER_DAY + timeOfDayNanos / NANOSECONDS_PER_SECOND;
        return new DecodedTimestamp(epochSeconds, (int) (timeOfDayNanos % NANOSECONDS_PER_SECOND));
    }
}
