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
package io.trino.parquet.writer.valuewriter;

import com.google.common.math.LongMath;
import io.trino.spi.block.Block;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.Type;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.schema.PrimitiveType;

import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static java.lang.Math.multiplyExact;
import static java.math.RoundingMode.UNNECESSARY;
import static java.util.Objects.requireNonNull;

public class TimestampNanosValueWriter
        extends PrimitiveValueWriter
{
    private final Type type;

    public TimestampNanosValueWriter(ValuesWriter valuesWriter, Type type, PrimitiveType parquetType)
    {
        super(parquetType, valuesWriter);
        this.type = requireNonNull(type, "type is null");
    }

    @Override
    public void write(Block block)
    {
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (!block.isNull(i)) {
                LongTimestamp value = (LongTimestamp) type.getObject(block, i);
                long epochNanos = multiplyExact(value.getEpochMicros(), NANOSECONDS_PER_MICROSECOND) +
                        LongMath.divide(value.getPicosOfMicro(), PICOSECONDS_PER_NANOSECOND, UNNECESSARY);
                getValueWriter().writeLong(epochNanos);
                getStatistics().updateStats(epochNanos);
            }
        }
    }
}
