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
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.trino.spi.block.Int128ArrayBlock.INT128_BYTES;

public class UuidValueWriter
        extends PrimitiveValueWriter
{
    private final byte[] buffer = new byte[INT128_BYTES];
    private final Slice reusedSlice = Slices.wrappedBuffer(buffer);
    private final Binary reusedBinary = Binary.fromReusedByteArray(buffer);

    public UuidValueWriter(ValuesWriter valuesWriter, PrimitiveType parquetType)
    {
        super(parquetType, valuesWriter);
    }

    @Override
    protected void writeValueBlock(ValueBlock block)
    {
        ValuesWriter valuesWriter = getValuesWriter();
        Statistics<?> statistics = getStatistics();
        Int128ArrayBlock int128ArrayBlock = (Int128ArrayBlock) block;
        boolean mayHaveNull = block.mayHaveNull();
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (!mayHaveNull || !block.isNull(i)) {
                writeToBuffer(int128ArrayBlock, i, reusedSlice);
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
        writeToBuffer((Int128ArrayBlock) block, 0, reusedSlice);
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
        for (int index = 0; index < length; index++) {
            int position = positions[offset + index];
            if (!mayHaveNull || !block.isNull(position)) {
                writeToBuffer(int128ArrayBlock, position, reusedSlice);
                valuesWriter.writeBytes(reusedSlice);
                statistics.updateStats(reusedBinary);
            }
        }
    }

    private static void writeToBuffer(Int128ArrayBlock block, int position, Slice slice)
    {
        slice.setLong(0, block.getInt128High(position));
        slice.setLong(SIZE_OF_LONG, block.getInt128Low(position));
    }
}
