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
package io.trino.server.protocol.spooling.encoding.arrow;

import io.trino.spi.block.Block;
import io.trino.spi.type.LongTimeWithTimeZone;
import io.trino.spi.type.TimeWithTimeZoneType;
import org.apache.arrow.vector.TimeSecVector;

import static io.trino.spi.type.DateTimeEncoding.unpackOffsetMinutes;
import static io.trino.spi.type.DateTimeEncoding.unpackTimeNanos;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MINUTE;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MINUTE;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_DAY;
import static java.lang.Math.floorMod;

public final class TimeWithTimeZoneSecWriter
        extends FixedWidthWriter<TimeSecVector>
{
    private final TimeWithTimeZoneType type;

    public TimeWithTimeZoneSecWriter(TimeSecVector vector, TimeWithTimeZoneType type)
    {
        super(vector);
        this.type = type;
    }

    @Override
    protected void setNull(int position)
    {
        vector.setNull(position);
    }

    @Override
    protected void writeValue(Block block, int position)
    {
        long normalizedNanos;
        if (type.isShort()) {
            long packed = type.getLong(block, position);
            long nanos = unpackTimeNanos(packed);
            int offsetMinutes = unpackOffsetMinutes(packed);
            normalizedNanos = floorMod(nanos - offsetMinutes * NANOSECONDS_PER_MINUTE, NANOSECONDS_PER_DAY);
        }
        else {
            LongTimeWithTimeZone time = (LongTimeWithTimeZone) type.getObject(block, position);
            long normalizedPicos = floorMod(time.getPicoseconds() - time.getOffsetMinutes() * PICOSECONDS_PER_MINUTE, PICOSECONDS_PER_DAY);
            normalizedNanos = normalizedPicos / 1000; // convert picoseconds to nanoseconds
        }
        
        vector.set(position, (int) (normalizedNanos / NANOSECONDS_PER_SECOND));
    }
} 