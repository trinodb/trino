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
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import org.apache.parquet.schema.PrimitiveType;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class Int32ShortDecimalValueWriter
        extends PrimitiveValueWriter
{
    public Int32ShortDecimalValueWriter(ValuesWriter valuesWriter, Type type, PrimitiveType parquetType)
    {
        super(parquetType, valuesWriter);
        DecimalType decimalType = (DecimalType) requireNonNull(type, "type is null");
        checkArgument(decimalType.getPrecision() <= 9, "decimalType precision %s must be <= 9", decimalType.getPrecision());
    }

    @Override
    protected void writeValueBlock(ValueBlock block)
    {
        LongArrayBlock longArrayBlock = (LongArrayBlock) block;
        int positionCount = block.getPositionCount();
        boolean mayHaveNull = block.mayHaveNull();
        int[] buffer = intBuffer();
        for (int start = 0; start < positionCount; start += buffer.length) {
            int end = min(start + buffer.length, positionCount);
            int size = 0;
            for (int position = start; position < end; position++) {
                if (!mayHaveNull || !block.isNull(position)) {
                    buffer[size] = toIntExact(longArrayBlock.getLong(position));
                    size++;
                }
            }
            writeIntegers(buffer, 0, size);
        }
    }

    @Override
    protected void writeRepeated(ValueBlock block, int count)
    {
        LongArrayBlock longArrayBlock = (LongArrayBlock) block;
        writeRepeatedInteger(toIntExact(longArrayBlock.getLong(0)), count);
    }

    @Override
    protected void writePositions(ValueBlock block, int[] positions, int offset, int length)
    {
        LongArrayBlock longArrayBlock = (LongArrayBlock) block;
        boolean mayHaveNull = block.mayHaveNull();
        int[] buffer = intBuffer();
        for (int start = 0; start < length; start += buffer.length) {
            int end = min(start + buffer.length, length);
            int size = 0;
            for (int index = start; index < end; index++) {
                int position = positions[offset + index];
                if (!mayHaveNull || !block.isNull(position)) {
                    buffer[size] = toIntExact(longArrayBlock.getLong(position));
                    size++;
                }
            }
            writeIntegers(buffer, 0, size);
        }
    }
}
