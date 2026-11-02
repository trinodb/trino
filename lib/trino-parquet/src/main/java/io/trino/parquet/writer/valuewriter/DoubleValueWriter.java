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

import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.ValueBlock;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.PrimitiveType;

import static java.lang.Double.longBitsToDouble;

public class DoubleValueWriter
        extends PrimitiveValueWriter
{
    public DoubleValueWriter(ValuesWriter valuesWriter, PrimitiveType parquetType)
    {
        super(parquetType, valuesWriter);
    }

    @Override
    protected void writeValueBlock(ValueBlock block)
    {
        ValuesWriter valuesWriter = getValuesWriter();
        Statistics<?> statistics = getStatistics();
        LongArrayBlock longArrayBlock = (LongArrayBlock) block;
        boolean mayHaveNull = block.mayHaveNull();
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (!mayHaveNull || !block.isNull(i)) {
                double value = longBitsToDouble(longArrayBlock.getLong(i));
                valuesWriter.writeDouble(value);
                statistics.updateStats(value);
            }
        }
    }

    @Override
    protected void writeRepeated(ValueBlock block, int count)
    {
        ValuesWriter valuesWriter = getValuesWriter();
        Statistics<?> statistics = getStatistics();
        double value = longBitsToDouble(((LongArrayBlock) block).getLong(0));
        for (int i = 0; i < count; i++) {
            valuesWriter.writeDouble(value);
        }
        statistics.updateStats(value);
    }

    @Override
    protected void writePositions(ValueBlock block, int[] positions, int offset, int length)
    {
        ValuesWriter valuesWriter = getValuesWriter();
        Statistics<?> statistics = getStatistics();
        LongArrayBlock longArrayBlock = (LongArrayBlock) block;
        boolean mayHaveNull = block.mayHaveNull();
        for (int index = 0; index < length; index++) {
            int position = positions[offset + index];
            if (!mayHaveNull || !block.isNull(position)) {
                double value = longBitsToDouble(longArrayBlock.getLong(position));
                valuesWriter.writeDouble(value);
                statistics.updateStats(value);
            }
        }
    }
}
