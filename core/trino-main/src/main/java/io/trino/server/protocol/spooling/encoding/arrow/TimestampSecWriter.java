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
import org.apache.arrow.vector.TimeStampSecVector;

import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;

public final class TimestampSecWriter
        extends FixedWidthWriter<TimeStampSecVector>
{
    public TimestampSecWriter(TimeStampSecVector vector)
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
        vector.set(position, TIMESTAMP_SECONDS.getLong(block, position) / 1_000_000);
    }
}
