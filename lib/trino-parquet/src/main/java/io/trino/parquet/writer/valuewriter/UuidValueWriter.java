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
import io.trino.spi.block.ValueBlock;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;

import static io.trino.spi.type.UuidType.UUID;

public class UuidValueWriter
        extends PrimitiveValueWriter
{
    public UuidValueWriter(ValuesWriter valuesWriter, PrimitiveType parquetType)
    {
        super(parquetType, valuesWriter);
    }

    @Override
    protected void writeValueBlock(ValueBlock block)
    {
        ValuesWriter valuesWriter = getValuesWriter();
        Statistics<?> statistics = getStatistics();
        boolean mayHaveNull = block.mayHaveNull();
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (!mayHaveNull || !block.isNull(i)) {
                Slice slice = UUID.getSlice(block, i);
                // fromReusedByteArray must be used instead of fromConstantByteArray to avoid retaining entire
                // base byte array of the Slice in DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter
                Binary binary = Binary.fromReusedByteArray(slice.byteArray(), slice.byteArrayOffset(), slice.length());
                valuesWriter.writeBytes(slice);
                statistics.updateStats(binary);
            }
        }
    }

    @Override
    protected void writeRepeated(ValueBlock block, int count)
    {
        ValuesWriter valuesWriter = getValuesWriter();
        Statistics<?> statistics = getStatistics();
        Slice slice = UUID.getSlice(block, 0);
        Binary binary = Binary.fromReusedByteArray(slice.byteArray(), slice.byteArrayOffset(), slice.length());
        for (int i = 0; i < count; i++) {
            valuesWriter.writeBytes(slice);
        }
        statistics.updateStats(binary);
    }

    @Override
    protected void writePositions(ValueBlock block, int[] positions, int offset, int length)
    {
        ValuesWriter valuesWriter = getValuesWriter();
        Statistics<?> statistics = getStatistics();
        boolean mayHaveNull = block.mayHaveNull();
        for (int index = 0; index < length; index++) {
            int position = positions[offset + index];
            if (!mayHaveNull || !block.isNull(position)) {
                Slice slice = UUID.getSlice(block, position);
                // fromReusedByteArray must be used instead of fromConstantByteArray to avoid retaining entire
                // base byte array of the Slice in DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter
                Binary binary = Binary.fromReusedByteArray(slice.byteArray(), slice.byteArrayOffset(), slice.length());
                valuesWriter.writeBytes(slice);
                statistics.updateStats(binary);
            }
        }
    }
}
