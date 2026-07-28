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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Int128ArrayBlock;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class FixedLenByteArrayLongDecimalValueWriter
        extends PrimitiveValueWriter
{
    public FixedLenByteArrayLongDecimalValueWriter(ValuesWriter valuesWriter, Type type, PrimitiveType parquetType)
    {
        super(parquetType, valuesWriter);
        DecimalType decimalType = (DecimalType) requireNonNull(type, "type is null");
        checkArgument(!decimalType.isShort(), "type is not a long decimal");
        checkArgument(
                parquetType.getTypeLength() > 0 && parquetType.getTypeLength() <= Int128.SIZE,
                "Type length %s must be in range 1-%s",
                parquetType.getTypeLength(),
                Int128.SIZE);
    }

    @Override
    protected void writeValueBlock(ValueBlock block)
    {
        ValuesWriter valuesWriter = getValuesWriter();
        Statistics<?> statistics = getStatistics();
        Int128ArrayBlock int128ArrayBlock = (Int128ArrayBlock) block;
        boolean mayHaveNull = block.mayHaveNull();
        byte[] buffer = new byte[getTypeLength()];
        Slice reusedSlice = Slices.wrappedBuffer(buffer);
        Binary reusedBinary = Binary.fromReusedByteArray(buffer);
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (!mayHaveNull || !block.isNull(position)) {
                storeInt128IntoBuffer(int128ArrayBlock, position, buffer);
                valuesWriter.writeBytes(reusedSlice);
                statistics.updateStats(reusedBinary);
            }
        }
    }

    @Override
    protected void writeRepeated(ValueBlock block, int count)
    {
        ValuesWriter valuesWriter = getValuesWriter();
        Statistics<?> statistics = getStatistics();
        byte[] buffer = new byte[getTypeLength()];
        Slice reusedSlice = Slices.wrappedBuffer(buffer);
        Binary reusedBinary = Binary.fromReusedByteArray(buffer);
        storeInt128IntoBuffer((Int128ArrayBlock) block, 0, buffer);
        for (int i = 0; i < count; i++) {
            valuesWriter.writeBytes(reusedSlice);
        }
        statistics.updateStats(reusedBinary);
    }

    @Override
    protected void writePositions(ValueBlock block, int[] positions, int offset, int length)
    {
        ValuesWriter valuesWriter = getValuesWriter();
        Statistics<?> statistics = getStatistics();
        Int128ArrayBlock int128ArrayBlock = (Int128ArrayBlock) block;
        boolean mayHaveNull = block.mayHaveNull();
        byte[] buffer = new byte[getTypeLength()];
        Slice reusedSlice = Slices.wrappedBuffer(buffer);
        Binary reusedBinary = Binary.fromReusedByteArray(buffer);
        for (int index = 0; index < length; index++) {
            int position = positions[offset + index];
            if (!mayHaveNull || !block.isNull(position)) {
                storeInt128IntoBuffer(int128ArrayBlock, position, buffer);
                valuesWriter.writeBytes(reusedSlice);
                statistics.updateStats(reusedBinary);
            }
        }
    }

    /**
     * Stores the two's complement value big-endian, truncated to the buffer length. The decimal
     * precision guarantees that the discarded leading bytes are sign extension.
     */
    private static void storeInt128IntoBuffer(Int128ArrayBlock block, int position, byte[] buffer)
    {
        long high = block.getInt128High(position);
        long low = block.getInt128Low(position);
        int length = buffer.length;
        int lowByteCount = Math.min(length, Long.BYTES);
        for (int i = 0; i < lowByteCount; i++) {
            buffer[length - 1 - i] = (byte) (low >> (Byte.SIZE * i));
        }
        for (int i = Long.BYTES; i < length; i++) {
            buffer[length - 1 - i] = (byte) (high >> (Byte.SIZE * (i - Long.BYTES)));
        }
    }
}
