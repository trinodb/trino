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
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;

import static java.util.Objects.requireNonNull;

public class BinaryValueWriter
        extends PrimitiveValueWriter
{
    private final Type type;

    public BinaryValueWriter(ValuesWriter valuesWriter, Type type, PrimitiveType parquetType)
    {
        super(parquetType, valuesWriter);
        this.type = requireNonNull(type, "type is null");
    }

    @Override
    public void write(Block block)
    {
        ValuesWriter valuesWriter = requireNonNull(getValuesWriter(), "valuesWriter is null");
        Statistics<?> statistics = requireNonNull(getStatistics(), "statistics is null");
        boolean mayHaveNull = block.mayHaveNull();
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (!mayHaveNull || !block.isNull(i)) {
                Slice slice = type.getSlice(block, i);
                // fromReusedByteArray must be used instead of fromConstantByteArray to avoid retaining entire
                // base byte array of the Slice in DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter
                Binary binary = Binary.fromReusedByteArray(slice.byteArray(), slice.byteArrayOffset(), slice.length());
                valuesWriter.writeBytes(binary);
                statistics.updateStats(binary);
            }
        }
    }
}
