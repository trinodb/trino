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
package io.prestosql.parquet.writer.valuewriter;

import io.prestosql.spi.block.Block;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.schema.PrimitiveType;

import static io.prestosql.spi.type.TimeType.TIME_MICROS;
import static io.prestosql.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;

public class TimeMicrosValueWriter
        extends PrimitiveValueWriter
{
    public TimeMicrosValueWriter(ValuesWriter valuesWriter, PrimitiveType parquetType)
    {
        super(parquetType, valuesWriter);
    }

    @Override
    public void write(Block block)
    {
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (!block.isNull(i)) {
                long scaledValue = TIME_MICROS.getLong(block, i) / PICOSECONDS_PER_MICROSECOND;
                getValueWriter().writeLong(scaledValue);
                getStatistics().updateStats(scaledValue);
            }
        }
    }
}
