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

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;
import org.joda.time.DateTimeZone;

import static io.trino.plugin.base.type.TrinoTimestampEncoderFactory.longTimestamp;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.round;

class LongTimestampEncoder
        extends AbstractTrinoTimestampEncoder<LongTimestamp>
{
    LongTimestampEncoder(TimestampType type, DateTimeZone timeZone)
    {
        super(type, timeZone);
    }

    @Override
    public void write(DecodedTimestamp decodedTimestamp, BlockBuilder blockBuilder)
    {
        LongTimestamp timestamp = getTimestamp(decodedTimestamp);
        type.writeObject(blockBuilder, timestamp);
    }

    @Override
    public LongTimestamp getTimestamp(DecodedTimestamp decodedTimestamp)
    {
        long adjustedSeconds = decodedTimestamp.epochSeconds();
        if (timeZone != DateTimeZone.UTC) {
            adjustedSeconds = timeZone.convertUTCToLocal(adjustedSeconds * MILLISECONDS_PER_SECOND) / MILLISECONDS_PER_SECOND;
        }
        int precision = type.getPrecision();
        int nanosOfSecond = decodedTimestamp.nanosOfSecond();
        if (precision < 9) {
            //noinspection NumericCastThatLosesPrecision
            nanosOfSecond = (int) round(nanosOfSecond, 9 - precision);
        }
        return longTimestamp(adjustedSeconds, ((long) nanosOfSecond) * PICOSECONDS_PER_NANOSECOND);
    }
}
