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

import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.ValueBlock;
import org.apache.parquet.schema.PrimitiveType;

import static java.lang.Math.min;

public class IntegerValueWriter
        extends PrimitiveValueWriter
{
    public IntegerValueWriter(ValuesWriter valuesWriter, PrimitiveType parquetType)
    {
        super(parquetType, valuesWriter);
    }

    @Override
    protected void writeValueBlock(ValueBlock block)
    {
        IntArrayBlock intArrayBlock = (IntArrayBlock) block;
        int positionCount = block.getPositionCount();
        if (!block.mayHaveNull()) {
            writeIntegers(intArrayBlock.getRawValues(), intArrayBlock.getRawValuesOffset(), positionCount);
            return;
        }
        int[] buffer = intBuffer();
        for (int start = 0; start < positionCount; start += buffer.length) {
            int end = min(start + buffer.length, positionCount);
            int size = 0;
            for (int position = start; position < end; position++) {
                if (!block.isNull(position)) {
                    buffer[size] = intArrayBlock.getInt(position);
                    size++;
                }
            }
            writeIntegers(buffer, 0, size);
        }
    }

    @Override
    protected void writeRepeated(ValueBlock block, int count)
    {
        IntArrayBlock intArrayBlock = (IntArrayBlock) block;
        writeRepeatedInteger(intArrayBlock.getInt(0), count);
    }

    @Override
    protected void writePositions(ValueBlock block, int[] positions, int offset, int length)
    {
        IntArrayBlock intArrayBlock = (IntArrayBlock) block;
        boolean mayHaveNull = block.mayHaveNull();
        int[] buffer = intBuffer();
        for (int start = 0; start < length; start += buffer.length) {
            int end = min(start + buffer.length, length);
            int size = 0;
            for (int index = start; index < end; index++) {
                int position = positions[offset + index];
                if (!mayHaveNull || !block.isNull(position)) {
                    buffer[size] = intArrayBlock.getInt(position);
                    size++;
                }
            }
            writeIntegers(buffer, 0, size);
        }
    }
}
