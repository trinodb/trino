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

import io.trino.spi.block.Block;
import io.trino.spi.type.LongTimestampWithTimeZone;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.schema.PrimitiveType;

import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.roundDiv;
import static java.util.Objects.requireNonNull;

public class TimestampTzMicrosValueWriter
        extends PrimitiveValueWriter
{
    public TimestampTzMicrosValueWriter(ValuesWriter valuesWriter, PrimitiveType parquetType)
    {
        super(parquetType, valuesWriter);
    }

    @Override
    public void write(Block block)
    {
        ValuesWriter valuesWriter = requireNonNull(getValuesWriter(), "valuesWriter is null");
        Statistics<?> statistics = requireNonNull(getStatistics(), "statistics is null");
        boolean mayHaveNull = block.mayHaveNull();
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (!mayHaveNull || !block.isNull(i)) {
                long micros = toMicros((LongTimestampWithTimeZone) TIMESTAMP_TZ_MICROS.getObject(block, i));
                valuesWriter.writeLong(micros);
                statistics.updateStats(micros);
            }
        }
    }

    private static long toMicros(LongTimestampWithTimeZone timestamp)
    {
        return (timestamp.getEpochMillis() * MICROSECONDS_PER_MILLISECOND) +
                roundDiv(timestamp.getPicosOfMilli(), PICOSECONDS_PER_MICROSECOND);
    }
}
