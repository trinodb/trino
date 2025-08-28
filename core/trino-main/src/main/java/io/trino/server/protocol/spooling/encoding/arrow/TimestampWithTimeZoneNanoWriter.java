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
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimestampWithTimeZoneType;
import org.apache.arrow.vector.TimeStampNanoTZVector;

import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;

public final class TimestampWithTimeZoneNanoWriter
        extends FixedWidthWriter<TimeStampNanoTZVector>
{
    private final TimestampWithTimeZoneType type;

    public TimestampWithTimeZoneNanoWriter(TimeStampNanoTZVector vector, TimestampWithTimeZoneType type)
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
        int precision = type.getPrecision();
        
        long epochMillis;
        int picosOfMilli = 0;
        if (precision <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
            long packed = type.getLong(block, position);
            epochMillis = unpackMillisUtc(packed);
        }
        else {
            LongTimestampWithTimeZone timestamp = (LongTimestampWithTimeZone) type.getObject(block, position);
            epochMillis = timestamp.getEpochMillis();
            picosOfMilli = timestamp.getPicosOfMilli();
        }
        
        vector.set(position, epochMillis * 1_000_000 + (picosOfMilli / 1_000));
    }
} 