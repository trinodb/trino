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
import io.trino.spi.block.Fixed12Block;
import io.trino.spi.block.ValueBlock;
import org.apache.parquet.schema.PrimitiveType;

import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static java.lang.Math.min;
import static java.lang.Math.multiplyExact;
import static java.math.RoundingMode.UNNECESSARY;

public class TimestampNanosValueWriter
        extends PrimitiveValueWriter
{
    public TimestampNanosValueWriter(ValuesWriter valuesWriter, PrimitiveType parquetType)
    {
        super(parquetType, valuesWriter);
    }

    @Override
    protected void writeValueBlock(ValueBlock block)
    {
        Fixed12Block fixed12Block = (Fixed12Block) block;
        int positionCount = block.getPositionCount();
        boolean mayHaveNull = block.mayHaveNull();
        long[] buffer = longBuffer();
        for (int start = 0; start < positionCount; start += buffer.length) {
            int end = min(start + buffer.length, positionCount);
            int size = 0;
            for (int position = start; position < end; position++) {
                if (!mayHaveNull || !block.isNull(position)) {
                    buffer[size] = toEpochNanos(fixed12Block, position);
                    size++;
                }
            }
            writeLongs(buffer, 0, size);
        }
    }

    @Override
    protected void writeRepeated(ValueBlock block, int count)
    {
        Fixed12Block fixed12Block = (Fixed12Block) block;
        writeRepeatedLong(toEpochNanos(fixed12Block, 0), count);
    }

    @Override
    protected void writePositions(ValueBlock block, int[] positions, int offset, int length)
    {
        Fixed12Block fixed12Block = (Fixed12Block) block;
        boolean mayHaveNull = block.mayHaveNull();
        long[] buffer = longBuffer();
        for (int start = 0; start < length; start += buffer.length) {
            int end = min(start + buffer.length, length);
            int size = 0;
            for (int index = start; index < end; index++) {
                int position = positions[offset + index];
                if (!mayHaveNull || !block.isNull(position)) {
                    buffer[size] = toEpochNanos(fixed12Block, position);
                    size++;
                }
            }
            writeLongs(buffer, 0, size);
        }
    }

    private static long toEpochNanos(Fixed12Block block, int position)
    {
        return multiplyExact(block.getFixed12First(position), NANOSECONDS_PER_MICROSECOND) +
                LongMath.divide(block.getFixed12Second(position), PICOSECONDS_PER_NANOSECOND, UNNECESSARY);
    }
}
