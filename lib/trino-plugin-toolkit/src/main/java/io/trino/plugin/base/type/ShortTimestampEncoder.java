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
import io.trino.spi.type.TimestampType;
import org.joda.time.DateTimeZone;

import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.round;
import static java.lang.Math.addExact;
import static java.lang.Math.multiplyExact;

class ShortTimestampEncoder
        extends AbstractTrinoTimestampEncoder<Long>
{
    ShortTimestampEncoder(TimestampType type, DateTimeZone timeZone)
    {
        super(type, timeZone);
    }

    @Override
    public void write(DecodedTimestamp decodedTimestamp, BlockBuilder blockBuilder)
    {
        Long micros = getTimestamp(decodedTimestamp);
        type.writeLong(blockBuilder, micros);
    }

    @Override
    public Long getTimestamp(DecodedTimestamp decodedTimestamp)
    {
        long micros;
        if (timeZone != DateTimeZone.UTC) {
            micros = multiplyExact(timeZone.convertUTCToLocal(multiplyExact(decodedTimestamp.epochSeconds(), MILLISECONDS_PER_SECOND)), MICROSECONDS_PER_MILLISECOND);
        }
        else {
            micros = multiplyExact(decodedTimestamp.epochSeconds(), MICROSECONDS_PER_SECOND);
        }
        int nanosOfSecond = (int) round(decodedTimestamp.nanosOfSecond(), 9 - type.getPrecision());
        return addExact(micros, nanosOfSecond / NANOSECONDS_PER_MICROSECOND);
    }
}
