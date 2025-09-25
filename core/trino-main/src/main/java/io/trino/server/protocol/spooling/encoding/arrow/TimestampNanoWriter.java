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
import io.trino.spi.type.LongTimestamp;
import org.apache.arrow.vector.TimeStampNanoVector;

import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static java.lang.Math.multiplyExact;

public final class TimestampNanoWriter
        extends FixedWidthWriter<TimeStampNanoVector>
{
    public TimestampNanoWriter(TimeStampNanoVector vector)
    {
        super(vector);
    }

    @Override
    protected void setNull(int position)
    {
        vector.setNull(position);
    }

    @Override
    protected void writeValue(Block block, int position)
    {
        LongTimestamp timestamp = (LongTimestamp) TIMESTAMP_NANOS.getObject(block, position);
        // Convert LongTimestamp to nanoseconds since epoch
        // For picosecond timestamps, epochMicros can be very large, so we need to check for overflow
        try {
            long epochNanos = multiplyExact(timestamp.getEpochMicros(), 1000L) +
                             (timestamp.getPicosOfMicro() / 1000L);
            vector.set(position, epochNanos);
        }
        catch (ArithmeticException e) {
            // Handle overflow by saturating to max/min values
            long epochMicros = timestamp.getEpochMicros();
            if (epochMicros > 0) {
                // Positive overflow - set to max representable nanoseconds
                vector.set(position, Long.MAX_VALUE);
            }
            else {
                // Negative overflow - set to min representable nanoseconds
                vector.set(position, Long.MIN_VALUE);
            }
        }
    }
}
