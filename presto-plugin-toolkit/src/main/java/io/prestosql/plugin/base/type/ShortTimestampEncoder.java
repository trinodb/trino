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
package io.prestosql.plugin.base.type;

import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.TimestampType;
import org.joda.time.DateTimeZone;

import static io.prestosql.plugin.base.type.PrestoTimestampEncoderFactory.scaleEpochMillisToMicros;
import static io.prestosql.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.prestosql.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.prestosql.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.prestosql.spi.type.Timestamps.round;

class ShortTimestampEncoder
        extends AbstractPrestoTimestampEncoder<Long>
{
    ShortTimestampEncoder(TimestampType type, DateTimeZone timeZone)
    {
        super(type, timeZone);
    }

    @Override
    public Long write(Long micros, BlockBuilder blockBuilder)
    {
        type.writeLong(blockBuilder, micros);
        return micros;
    }

    @Override
    public Long getTimestamp(DecodedTimestamp decodedTimestamp)
    {
        long micros;
        if (timeZone != DateTimeZone.UTC) {
            micros = scaleEpochMillisToMicros(timeZone.convertUTCToLocal(decodedTimestamp.getEpochSeconds() * MILLISECONDS_PER_SECOND));
        }
        else {
            micros = decodedTimestamp.getEpochSeconds() * MICROSECONDS_PER_SECOND;
        }
        int nanosOfSecond = (int) round(decodedTimestamp.getNanosOfSecond(), 9 - type.getPrecision());
        return micros + nanosOfSecond / NANOSECONDS_PER_MICROSECOND;
    }
}
