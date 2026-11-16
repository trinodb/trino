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
import org.apache.parquet.schema.PrimitiveType;

import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static java.lang.Math.floorDiv;
import static java.lang.Math.min;

public class TimestampMillisValueWriter
        extends PrimitiveValueWriter
{
    public TimestampMillisValueWriter(ValuesWriter valuesWriter, PrimitiveType parquetType)
    {
        super(parquetType, valuesWriter);
    }

    @Override
    protected void writeValueBlock(ValueBlock block)
    {
        LongArrayBlock longArrayBlock = (LongArrayBlock) block;
        int positionCount = block.getPositionCount();
        boolean mayHaveNull = block.mayHaveNull();
        long[] buffer = longBuffer();
        for (int start = 0; start < positionCount; start += buffer.length) {
            int end = min(start + buffer.length, positionCount);
            int size = 0;
            for (int position = start; position < end; position++) {
                if (!mayHaveNull || !block.isNull(position)) {
                    buffer[size] = floorDiv(longArrayBlock.getLong(position), MICROSECONDS_PER_MILLISECOND);
                    size++;
                }
            }
            writeLongs(buffer, 0, size);
        }
    }

    @Override
    protected void writeRepeated(ValueBlock block, int count)
    {
        LongArrayBlock longArrayBlock = (LongArrayBlock) block;
        writeRepeatedLong(floorDiv(longArrayBlock.getLong(0), MICROSECONDS_PER_MILLISECOND), count);
    }

    @Override
    protected void writePositions(ValueBlock block, int[] positions, int offset, int length)
    {
        LongArrayBlock longArrayBlock = (LongArrayBlock) block;
        boolean mayHaveNull = block.mayHaveNull();
        long[] buffer = longBuffer();
        for (int start = 0; start < length; start += buffer.length) {
            int end = min(start + buffer.length, length);
            int size = 0;
            for (int index = start; index < end; index++) {
                int position = positions[offset + index];
                if (!mayHaveNull || !block.isNull(position)) {
                    buffer[size] = floorDiv(longArrayBlock.getLong(position), MICROSECONDS_PER_MILLISECOND);
                    size++;
                }
            }
            writeLongs(buffer, 0, size);
        }
    }
}
