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

import io.trino.spi.block.ValueBlock;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.PrimitiveType;

import static io.trino.spi.type.DateType.DATE;

public class DateValueWriter
        extends PrimitiveValueWriter
{
    public DateValueWriter(ValuesWriter valuesWriter, PrimitiveType parquetType)
    {
        super(parquetType, valuesWriter);
    }

    @Override
    protected void writeValueBlock(ValueBlock block)
    {
        ValuesWriter valuesWriter = getValuesWriter();
        Statistics<?> statistics = getStatistics();
        boolean mayHaveNull = block.mayHaveNull();
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (!mayHaveNull || !block.isNull(position)) {
                int value = DATE.getInt(block, position);
                valuesWriter.writeInteger(value);
                statistics.updateStats(value);
            }
        }
    }

    @Override
    protected void writeRepeated(ValueBlock block, int count)
    {
        ValuesWriter valuesWriter = getValuesWriter();
        Statistics<?> statistics = getStatistics();
        int value = DATE.getInt(block, 0);
        for (int i = 0; i < count; i++) {
            valuesWriter.writeInteger(value);
        }
        statistics.updateStats(value);
    }

    @Override
    protected void writePositions(ValueBlock block, int[] positions, int offset, int length)
    {
        ValuesWriter valuesWriter = getValuesWriter();
        Statistics<?> statistics = getStatistics();
        boolean mayHaveNull = block.mayHaveNull();
        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            if (!mayHaveNull || !block.isNull(position)) {
                int value = DATE.getInt(block, position);
                valuesWriter.writeInteger(value);
                statistics.updateStats(value);
            }
        }
    }
}
