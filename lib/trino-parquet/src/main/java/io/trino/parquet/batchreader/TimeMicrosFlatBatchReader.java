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
package io.trino.parquet.batchreader;

import io.trino.parquet.Field;
import io.trino.parquet.reader.ColumnChunk;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.Timestamps;
import io.trino.spi.type.Type;
import org.apache.parquet.column.ColumnDescriptor;

import java.util.Optional;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;

public class TimeMicrosFlatBatchReader
        extends LongFlatBatchReader
{
    public TimeMicrosFlatBatchReader(ColumnDescriptor descriptor)
    {
        super(descriptor);
    }

    @Override
    protected ColumnChunk makeColumnChunk(Field field, int totalNonNullCount, int batchSize, boolean[] isNull)
    {
        if (totalNonNullCount == 0) {
            Block block = RunLengthEncodedBlock.create(field.getType(), null, batchSize);
            return new ColumnChunk(block, emptyIntArray, emptyIntArray);
        }

        boolean hasNoNull = totalNonNullCount == batchSize;
        Optional<boolean[]> nullArr = hasNoNull ? Optional.empty() : Optional.of(isNull);
        Block block;

        // TODO: Move conversion into readValues to avoid wasting time converting nulls
        Type type = field.getType();
        if (type instanceof TimeType) {
            for (int i = 0; i < batchSize; i++) {
                values[i] *= Timestamps.PICOSECONDS_PER_MICROSECOND;
            }
            block = new LongArrayBlock(batchSize, nullArr, values);
        }
        else {
            throw new TrinoException(NOT_SUPPORTED, format("Unsupported Trino column type (%s) for Parquet column (%s)", type, columnDescriptor));
        }

        return new ColumnChunk(block, emptyIntArray, emptyIntArray);
    }
}
