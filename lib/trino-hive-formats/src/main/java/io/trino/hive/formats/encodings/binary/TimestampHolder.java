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
package io.trino.hive.formats.encodings.binary;

import io.trino.spi.block.Block;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;

import java.util.function.BiFunction;

import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.toIntExact;

final class TimestampHolder
{
    private final long seconds;
    private final int nanosOfSecond;

    private TimestampHolder(long epochMicros, int picosOfMicro)
    {
        this.seconds = floorDiv(epochMicros, MICROSECONDS_PER_SECOND);
        long picosOfSecond = (long) floorMod(epochMicros, MICROSECONDS_PER_SECOND) * PICOSECONDS_PER_MICROSECOND + picosOfMicro;

        // no rounding since the data has nanosecond precision, at most
        this.nanosOfSecond = toIntExact(picosOfSecond / PICOSECONDS_PER_NANOSECOND);
    }

    public long getSeconds()
    {
        return seconds;
    }

    public int getNanosOfSecond()
    {
        return nanosOfSecond;
    }

    public static BiFunction<Block, Integer, TimestampHolder> getFactory(TimestampType type)
    {
        if (type.isShort()) {
            return (block, position) -> new TimestampHolder(type.getLong(block, position), 0);
        }
        return (block, position) -> {
            LongTimestamp longTimestamp = (LongTimestamp) type.getObject(block, position);
            return new TimestampHolder(longTimestamp.getEpochMicros(), longTimestamp.getPicosOfMicro());
        };
    }
}
